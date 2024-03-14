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

-- MAGIC %md
-- MAGIC
