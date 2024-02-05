-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Raw Kafka Views - Silver

-- COMMAND ----------

-- DBTITLE 1,Kafka Transactions Silver
create or replace temporary view Kafka_Transactions AS

with exploded_st as (
SELECT
        t.id,
        stateTransition.successful.providerPaymentReference AS providerPaymentReference
    FROM
        bolt_finint_prod.silver.fi_transactionv2_enriched t
    LATERAL VIEW
        explode(t.stateTransitions) stateTransitionsTable AS stateTransition
    WHERE
        stateTransition.successful IS NOT NULL
)

select
t.created::date as created_date,
t.currentstate.occurred::date as occurred_date,

case
  when t.type = 'PAYMENT_TRANSACTION_TYPE_AUTOMATED' then 'RECURRING'
  when t.type = 'PAYMENT_TRANSACTION_TYPE_INTERACTIVE' then 'FIRST'
  when t.type = 'PAYMENT_TRANSACTION_TYPE_INTERACTIVE' AND t.amountDetails.amountWithTax.minorAmountInclTax = 0 then 'FIRST_FREE'
  else 'UNKNOWN'
end as payment_type,

case
  when (t.currentState.refunded.providerRefundReference is not null 
        OR t.currentState.successful.providerPaymentReference is not null 
        OR t.currentState.chargeback.providerChargebackReference is not null
        OR t.currentState.chargebackrejected.providerChargebackReference is not null)
  then 'SUCCESS'
  when t.currentState.pending.providerPaymentReference is not null then 'PENDING'
  when t.currentState.canceled.reason is not null then 'CANCELLED'
  when t.currentState.failed.providerPaymentReference is not null OR t.currentState.failed.reason is not null OR t.currentState.retrying.reason is not null then 'FAILED'
  when t.currentState.revoked.reason is not null then 'REVOKED'
  else 'ERROR'
end as Transaction_status,

case
  when t.currentState.refunded.providerRefundReference is not null then 'REFUND'
  when t.currentState.chargeback.providerChargebackReference is not null then 'CHARGEBACK'
  else 'CHARGE'
end as Transaction_type,

pr.id as product_id,
REPLACE(pr.tiertype,'TIER_TYPE_','') as Tier_type,
REPLACE(pp.period,'PERIOD_','') as payment_period,
pr.name as product_name,
pr.description as product_description,
to_date(regexp_extract(pr.description, '(\\d{4}-\\d{2}-\\d{2}).*(\\d{4}-\\d{2}-\\d{2})', 1),
               'yyyy-MM-dd')                          as invoice_period_start,
to_date(regexp_extract(pr.description, '(\\d{4}-\\d{2}-\\d{2}).*(\\d{4}-\\d{2}-\\d{2})', 2),
               'yyyy-MM-dd')                          as invoice_period_end,

pp.id as price_plan_id,
pp.internalname as internal_name,
c.name as campaign_name,
pp.price/100 as plan_price,
(t.amountDetails.amountWithTax.minorAmountInclTax - coalesce(t.amountDetails.amountwithtax.taxminoramount,0)) /100 as amount_before_tax,
coalesce(t.amountDetails.amountwithtax.taxminoramount,0)/100 as sales_tax_amount,

case 
  when (t.currentState.refunded.providerRefundReference is not null OR t.currentState.chargeback.providerChargebackReference is not null) 
    then (ABS(t.amountDetails.amountWithTax.minorAmountInclTax) / 100)*-1
  else ABS(t.amountDetails.amountWithTax.minorAmountInclTax) / 100
end as charged_amount,
t.amountDetails.currencyCode as currency,

case
  when t.currentState.refunded.providerRefundReference is not null then t.currentState.refunded.providerRefundReference
  when t.currentState.chargeback.providerChargebackReference is not null then t.currentState.chargeback.providerChargebackReference
  when t.currentState.successful.providerPaymentReference is not null then t.currentState.successful.providerPaymentReference
end as provider_reference_id,

case 
  when t.currentState.refunded.refundReference is null then t.currentState.chargeback.providerChargebackReference 
  else t.currentState.refunded.refundReference 
end as previous_trx_id,

t.currentState.refunded.source as Refund_source,
case 
  when t.currentState.successful.providerPaymentReference is not null then null 
  else ex.providerPaymentReference 
end as og_provider_reference_id,

case
  when t.provider = 'PAYMENT_PROVIDER_STRIPE' then 'STRIPE'
  when t.provider = 'PAYMENT_PROVIDER_PAYPAL' then 'PAYPAL'
end as Payment_provider,

case
  when t.provider = 'PAYMENT_PROVIDER_PAYPAL'  then 'PAYPAL' 
  else UPPER(pm.cardDetails.cardProvider) 
end as Card_provider,

pm.cardDetails.cardPaymentMethod as Card_pm,
pm.cardDetails.cardIssuingBank as card_bank,
case
  when t.provider = 'PAYMENT_PROVIDER_PAYPAL' then 'PAYPAL' 
  else UPPER(pm.cardDetails.fundingSource)
end as card_funding,

pm.countryCode as Payment_Country,
pm.geoLocation.countryIso as pm_countryiso,
pm.realm as Realm,

t.userId as customer_userid,

case 
  when t.source.type = 'subscription' then t.source.reference
end as globalsusbcriptionid,

t.id as trx_id

from bolt_finint_prod.silver.fi_transactionv2_enriched t
LEFT JOIN exploded_st as ex on t.id = ex.id
LEFT JOIN bolt_finint_prod.silver.fi_paymentmethodv2_enriched pm ON t.paymentMethodId = pm.id
LEFT JOIN bolt_finint_prod.silver.fi_subscriptionv2_enriched s ON t.source.reference=s.globalSubscriptionId
LEFT JOIN bolt_finint_prod.silver.fi_priceplanv2_enriched pp ON s.pricePlanId = pp.id
LEFT JOIN bolt_finint_prod.silver.fi_product_enriched pr ON pp.productid = pr.id
LEFT JOIN bolt_finint_prod.silver.fi_campaignv2_enriched c ON pp.campaignId = c.id

where 0=0
 group by all

-- COMMAND ----------

-- MAGIC
-- MAGIC %md # Control Reporting

-- COMMAND ----------

-- DBTITLE 1,Duplicate DTC Transactions - Kafka
---\/ identify refunds separately to join to the transactions later \/---
with refunds AS (
  SELECT previous_trx_id,
         SUM(charged_amount) AS total_refunded
  FROM Kafka_Transactions
  WHERE Transaction_type = 'REFUND' OR Transaction_type = 'CHARGEBACK'
  GROUP BY previous_trx_id
)

--\/ final statement \/--
SELECT customer_userid,
       created_date,
       product_name,
       coalesce(tr.productDescription, 'N/a') as product_desc,
       Plan_price,
       COUNT(distinct trx_id)                           as count,
       COALESCE(SUM(charged_amount), 0)         as Total_Charges,
       count(refunds.previous_trx_id)           as Refunds_count,
       COALESCE(SUM(refunds.total_refunded), 0) as total_refunded
FROM Kafka_Transactions kt
         LEFT JOIN refunds ON kt.trx_id = refunds.previous_trx_id
         LEFT JOIN bolt_finint_prod.silver.fi_transaction_enriched tr ON kt.trx_id = tr.merchantReferenceId
where 1=1
    AND created_date >= '2023-10-01'
    AND transaction_status = 'SUCCESS'
    AND transaction_type = 'CHARGE'
    AND payment_provider IN ('STRIPE','PAYPAL')
GROUP BY 1, 2, 3, 4, 5
HAVING count > 1 --returns duplicate transactions only--
and Refunds_count = 0 --removes already refunded users - comment out this line to see full log--


-- COMMAND ----------

-- DBTITLE 1,Duplicate DTC Subscriptions - Kafka Raw Events
select
record.subscription.userid as userid,
-- record.subscription.status as status,
count(record.subscription.globalsubscriptionid) as sub_count
from bolt_dcp_prod.bronze.raw_s2s_subscription_entities
where 1=1
  and UPPER(record.subscription.direct.paymentprovider) in ('SPREEDLY','PAYPAL')
  and record.subscription.partner is null
  and record.subscription.status in ('STATUS_ACTIVE','STATUS_CANCELED')
group by 1
HAVING sub_count > 1

-- COMMAND ----------

-- DBTITLE 1,Unbilled Susbcriptions - Kafka Silver
Select
  s.userid,
  s.globalsubscriptionid,
  s.status as sub_status,
  pr.name as product_name,
  replace(pp.period, "PERIOD_","") as payment_period,
  pp.internalname as internal_name,
  coalesce(pp.price/100,0) as sku_price,
  s.startDate :: date as sub_start_date,
  s.nextRenewalDate :: date as next_renewal_date
from
  bolt_finint_prod.silver.fi_subscriptionv2_enriched s
  join bolt_finint_prod.silver.fi_priceplanv2_enriched pp on s.pricePlanId = pp.id
  join bolt_finint_prod.silver.fi_product_enriched pr on pp.productId = pr.id
where
  1 = 1
  and direct.paymentprovider is not null
  and nextrenewaldate :: date < current_date() -18
  and s.status in ('STATUS_ACTIVE', 'STATUS_CANCELED')
  and pp.period != 'PERIOD_YEAR'
group by
  all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ad-hoc

-- COMMAND ----------

-- DBTITLE 1,Refund Audit
select
ra.realm,
ra.action,
ra.created::date as refund_date,
ra.transactionId,
ra.refunderUserId,
us.unpackedvalue.user.email as refunder_email,
ra.reasonOfRefund,
ra.customerUserId,
ra.amount/100 as amount,
ra.currency,
ra.paymentMethod.id as payment_method_id,
ra.paymentMethod.paymentType as payment_type,
ra.paymentMethod.provider as Provider
from bolt_finint_prod.silver.fi_refundaudit_enriched ra
LEFT JOIN bolt_finint_prod.silver.s2s_user_entities us ON ra.refunderUserId=us.unpackedValue.user.userid
where created::date >= '2023-05-22'

-- COMMAND ----------

-- DBTITLE 1,Vertex check
with vertex_charges as (
  Select 
  `Posting Date`,
  `Flex Code 12` as bolt_user_id,
  `Transaction Synchronization ID`,
  SUM(`Gross Amount`) as gross_amt, 
  SUM(`Tax Amount`) as Tax_amt
from bolt_finint_int.bronze.vertex_extract_v3
group by 1,2,3
HAVING gross_amt > 0)
,
kafka_charges as (
  select *
  from Kafka_Transactions
  where transaction_status = 'SUCCESS'
  -- and transaction_type IN ('CHARGE','CHARGEBACK')
)
,
Kinesis_charges as (
  select *
  from bolt_finint_prod.silver.fi_transaction_enriched
  where upper(status) = 'SUCCESS'
)

Select v.*,
kf.customer_userid,
kf.trx_id,
kf.refund_source,
kf.plan_price,
kf.amount_before_tax,
kf.sales_tax_amount,
ki.userId,
ki.merchantReferenceId,
ki.id,
ki.refundedTransactionId,
ki.amount
from vertex_charges v
Left join kafka_charges kf
  on v.`Transaction Synchronization ID`= kf.trx_id
Left Join kinesis_charges ki
  on v.`Transaction Synchronization ID`= ki.merchantReferenceId
  group by all

-- COMMAND ----------

select
created_date,
payment_Provider,
card_provider,
card_funding,
count(trx_id) as quantity,
sum(charged_amount) as total_amount 
from kafka_transactions
where created_date BETWEEN '2023-12-01' AND '2023-12-31'
and transaction_status = 'SUCCESS'
and transaction_type = 'CHARGE'
group by 1,2,3,4

-- COMMAND ----------

select
  case
    when marketid = 'TG_MARKET_UNITED_STATES' then 'AMER'
    else 'LATAM'
  end as Region,
  marketId as market,
  pp.id as priceplanid,
  pr.addOnIds [0] as addOnIds,
  pr.mainProductIds [0] as mainProductIds,
  pr.productType as producttype,
  pr.name as productname,
  pr.tiertype as tiertype,
  if(pp.provider = 'WEB', 'Direct', provider) as Provider,
  ca.name as campaignName,
  pp.internalName as internalname,
  pp.pricePlanType as priceplantype,
  pp.period as paymentPeriod,
  pp.numberOfInstalments,
  pp.numberOfPeriodsBetweenPayments,
  coalesce(pp.price, 0) as price,
  pp.currencyDecimalPoints,
  CASE
    WHEN pp.currencyDecimalPoints IS NOT NULL THEN pp.price / POWER(10, pp.currencyDecimalPoints)
    ELSE coalesce(pp.price, 0)
  END AS ConvertedPrice,
  pp.currency as currency
from
  bolt_finint_prod.silver.fi_priceplanv2_enriched pp
  LEFT JOIN bolt_finint_prod.silver.fi_product_enriched pr on pp.productid = pr.id
  LEFT JOIN bolt_finint_prod.silver.fi_campaignv2_enriched ca on pp.id = ca.pricePlanId
where marketid != 'TG_MARKET_UNITED_STATES'
group by
  all


-- COMMAND ----------

select t.priceplanid, pp.market
from bolt_finint_prod.silver.fi_transactionv2_enriched t
left join bolt_finint_prod.silver.fi_priceplanv2_enriched pp on t.priceplanid=pp.id
where source.reference IN
('936af8ca-9111-5399-8e3f-171dd46f5072',
'd7f36380-a2ab-5934-aca1-0c020558f004',
'35b186c2-0743-5520-9c0f-79543d3a0c1d',
'037dc91a-a118-5f2f-9626-e48fa6b574ca',
'ff16012a-c024-5cff-8633-610468d3baa9',
'507dcf0c-6e99-5a8f-a99e-bda8df54f5a3',
'7f948130-caa1-57ee-bb18-bfa5ed29939e',
'e014b8c5-427d-5c64-aa9e-fede1588f793',
'5fed6f32-a407-5245-8193-629b69111b6a',
'4c2ac868-9ca6-5bb7-9ec4-1daddb36da98')
group by 1,2

