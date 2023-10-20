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
t.created::date as transaction_date,

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
LEFT JOIN bolt_finint_prod.silver.fi_priceplan_enriched_kafka pp ON s.pricePlanId = pp.id
LEFT JOIN bolt_finint_prod.silver.fi_product_enriched pr ON pp.productid = pr.id
LEFT JOIN bolt_finint_prod.silver.fi_campaignv2_enriched c ON pp.campaignId = c.id

where 0=0
 group by all

-- COMMAND ----------

-- DBTITLE 1,Kafka Subscriptions Silver
create or replace temporary view Kafka_Subscriptions AS
---\/ pulls together the priceplan and product tables for subscription info \/---

with plan as (
select
  pp.id as priceplanid,
  pr.name as product_name,
  case
    when UPPER(pr.name) like '%MVPD%' then 'Partner'
    when pp.provider = 'WEB' or internalname like 'US Web%' then 'Direct'
    when priceplantype = 'TYPE_THIRD_PARTY' then 'IAP'
    else 'Direct'
  end as Sub_source,
  case
    when pr.tiertype = 'TIER_TYPE_AD_FREE' then 'Ad-Free'
    when pr.tiertype = 'TIER_TYPE_AD_LITE' then 'Ad-Lite'
    when pr.tiertype = 'TIER_TYPE_PREMIUM_AD_FREE' then 'Ultimate'
    else 'Unclassified'
  end as tiertype,
  case
    when pp.provider != 'WEB' then pp.provider
    else 'Direct'
  end as provider_name,
  pp.internalName as internalname,
  pp.campaignId,
  ca.name as campaign_name,
  replace(pr.productPrices [0].market, 'TG_MARKET_', '') as market,
  replace(pp.pricePlanType, 'TYPE_', '') as priceplantype,
  replace(pp.period, 'PERIOD_', '') as paymentPeriod,
  Coalesce(pp.price / 100,0) as price,
  pp.currency as currency
from
  bolt_finint_prod.silver.fi_priceplan_enriched_kafka pp
  JOIN bolt_finint_prod.silver.fi_product_enriched pr on pp.productid = pr.id
  JOIN bolt_finint_prod.silver.fi_campaignv2_enriched ca on pp.campaignId = ca.id
)


---\/ Final Statement \/---
select

plan.provider_name,
plan.tiertype as tier_type,
replace(s.direct.subscriptiontype, 'TYPE_', '') as subscription_type,

s.status as Subscription_Status,
s.startdate::date as Sub_Start_date,
s.enddate::date as Sub_End_date,
s.nextRenewaldate::date as Next_Renewal_Date,

---\/ Creates the current invoice period start date for revenue recognition accrual \/---
CASE
 WHEN plan.paymentPeriod = 'MONTH' THEN ADD_MONTHS(s.nextRenewalDate, -1)
 WHEN plan.paymentPeriod = 'YEAR' THEN ADD_MONTHS(s.nextRenewalDate, -12)
END AS invoice_period_start,

plan.product_name,
plan.paymentPeriod,
plan.priceplantype,
plan.price,

---\/ cleanup for payment method info \/---
case
  when pm.provider LIKE '%SPREEDLY%' then 'STRIPE'
  when pm.provider LIKE '%PAYPAL%' then 'PAYPAL'
end as payment_provider,
UPPER(pm.cardDetails.cardProvider) as card_Brand,
UPPER(pm.cardDetails.fundingSource) as card_funding,

count(*) as Subscriber_count


from bolt_finint_prod.silver.fi_subscriptionv2_enriched s
LEFT JOIN plan on s.priceplanid=plan.priceplanid
LEFT JOIN bolt_finint_prod.silver.fi_paymentmethodv2_enriched pm on s.direct.paymentMethodId = pm.legacyPaymentMethodId
where 1=1 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

-- COMMAND ----------

-- MAGIC
-- MAGIC %md # Control Reporting

-- COMMAND ----------

-- DBTITLE 1,Duplicate Transactions - Kafka
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
       transaction_date,
       product_name,
       Plan_price,
       COUNT(trx_id)                            as count,
       COALESCE(SUM(charged_amount), 0)         as Total_Charges,
       count(refunds.previous_trx_id)           as Refunds_count,
       COALESCE(SUM(refunds.total_refunded), 0) as total_refunded
FROM Kafka_Transactions kt
         LEFT JOIN refunds ON kt.trx_id = refunds.previous_trx_id
         LEFT JOIN bolt_finint_prod.silver.fi_transaction_enriched tr ON kt.trx_id = tr.merchantReferenceId
where 1=1
    -- AND transaction_date >= current_date()-1
    AND transaction_date >= '2023-10-14'
    AND transaction_status = 'SUCCESS'
    AND transaction_type = 'CHARGE'
    AND payment_provider IN ('STRIPE','PAYPAL')
GROUP BY 1, 2, 3, 4
HAVING count > 1 --returns duplicate transactions only--
and Refunds_count = 0 --removes already refunded users - comment out this line to see full log--

