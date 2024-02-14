-- Databricks notebook source
-- MAGIC %md
-- MAGIC #LATAM Testing Queries

-- COMMAND ----------

-- DBTITLE 1,Kafka Transactions - LATAM Only
select

t.record.transaction.created::date as record_created_date,
t.record.transaction.stateTransitions[0].occurred::date as st_occurred_date,
t.record.transaction.currentstate.occurred::date as cs_occurred_date,
t.record.transaction.userid,
u.record.`user`.email as email,
t.record.transaction.source.type as source_type,
t.record.transaction.source.reference as source_reference, 
t.record.transaction.id as transaction_id,
t.record.transaction.stateTransitions[0].successful.providerPaymentReference,
t.record.transaction.currentstate.successful.providerPaymentReference,
ki.vendorReferenceId as vendorref_kinesis,
pr.unpackedValue.product.name as product_name,
pr.unpackedValue.product.description as product_desc,
pr.unpackedValue.product.productprices[0].period as product_period,
pp.unpackedValue.pricePlan.period as plan_period,
pp.unpackedValue.pricePlan.internalname as plan_internal_name,

  CASE
    WHEN pp.unpackedValue.pricePlan.currencyDecimalPoints IS NOT NULL THEN pp.unpackedValue.pricePlan.price / POWER(10, pp.unpackedValue.pricePlan.currencyDecimalPoints)
    ELSE coalesce(pp.unpackedValue.pricePlan.price, 0)
  END AS Plan_Price,

  CASE
    WHEN pp.unpackedValue.pricePlan.currencyDecimalPoints IS NOT NULL THEN t.record.transaction.amountdetails.amountWithTax.minorAmountInclTax / POWER(10, pp.unpackedValue.pricePlan.currencyDecimalPoints)
    ELSE coalesce(t.record.transaction.amountdetails.amountWithTax.minorAmountInclTax, 0)
  END AS charged_amount,

  CASE
    WHEN pp.unpackedValue.pricePlan.currencyDecimalPoints IS NOT NULL THEN t.record.transaction.amountdetails.amountWithTax.taxMinorAmount / POWER(10, pp.unpackedValue.pricePlan.currencyDecimalPoints)
    ELSE coalesce(t.record.transaction.amountdetails.amountWithTax.taxMinorAmount, 0)
  END AS tax_amount,

t.record.transaction.amountdetails.amountWithTax.taxRateMinorUnits/10000 as tax_rate,
t.record.transaction.amountdetails.amountWithTax.components[0].description as Tax_description,
-- t.record.transaction.amountdetails.amountWithTax.taxDocumentReference,
t.record.transaction.amountdetails.currencyCode,
t.record.transaction.items[0].subscriptionDetails.serviceperiod.startdate::date as invoice_start_date,
t.record.transaction.items[0].subscriptionDetails.serviceperiod.enddate::Date as invoice_end_date,
t.record.transaction.invoiceid

from bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_transaction_entities t

left join bolt_dcp_prod.beam_latam_bronze.raw_s2s_subscription_entities s
  on t.record.transaction.source.reference=s.record.subscription.globalSubscriptionId

left join bolt_payments_prod.gold.s2s_price_plan_entities pp
  on pp.unpackedValue.pricePlan.id=s.record.subscription.priceplanid
  
left join bolt_payments_prod.gold.s2s_product_catalog_entities pr
  on pp.unpackedValue.pricePlan.productid=pr.unpackedValue.product.id

left join bolt_dcp_prod.beam_latam_bronze.raw_s2s_user_entities u
  on t.record.transaction.userid=u.record.`user`.userid

left join bolt_finint_prod.latam_silver.fi_transaction_enriched ki
  on t.record.transaction.id = ki.merchantReferenceId

where 1=1
-- and t.record.transaction.stateTransitions[0].occurred::date >= '2023-12-08'
-- and lower(pr.unpackedValue.product.name) not like ('%legacy%')
AND t.record.transaction.userid = 'USERID:bolt:7b930568-36da-4d93-81ca-c321b47a4e9d'
-- AND t.record.transaction.invoiceid is not null

-- COMMAND ----------

select record.transaction.stateTransitions, record.transaction.currentState
from bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_transaction_entities
where record.transaction.userid = 'USERID:bolt:2e5f7ca9-d311-40ad-9bd1-28e561709f50'
