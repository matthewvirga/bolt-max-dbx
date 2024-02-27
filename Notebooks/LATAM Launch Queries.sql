-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Kafka Main Views

-- COMMAND ----------

-- DBTITLE 1,Latam Plans
create or replace temporary view Plans as
(
select
pp.realm,
  case
    when marketid = 'TG_MARKET_UNITED_STATES' then 'AMER'
    else 'LATAM'
  end as Region,
  marketId as market,
  pp.id as priceplanid,
  -- pr.addOnIds [0] as addOnIds,
  -- pr.mainProductIds [0] as mainProductIds,
  pr.productType as producttype,
  pr.name as productname,
  pr.tiertype as tiertype,
  pp.provider as Provider,
  ca.name as campaignName,
  pp.internalName as internalname,
  pp.pricePlanType as priceplantype,
  pp.retentionOffer as retention_offer,
  pp.period as paymentPeriod,
  pp.numberOfInstalments,
  pp.numberOfPeriodsBetweenPayments,
  coalesce(pp.price, 0) as price,
  coalesce(pp.currencyDecimalPoints,0) as currencyDecimalPoints,
  coalesce(pp.price, 0) / POWER(10, coalesce(pp.currencyDecimalPoints,0)) AS plan_price,
  pp.currency as currency
from
  bolt_finint_prod.silver.fi_priceplanv2_enriched pp
  LEFT JOIN bolt_finint_prod.silver.fi_product_enriched pr on pp.productid = pr.id
  LEFT JOIN bolt_finint_prod.silver.fi_campaignv2_enriched ca on pp.id = ca.pricePlanId
group by
  all
)

-- COMMAND ----------

-- DBTITLE 1,Subscriptions
create or replace temporary view Subscriptions as
(
select
realm,
globalSubscriptionId,
userId,
status,
startDate,
pricePlanId,
nextRenewalDate,
endDate,
cancellationDate,
terminationDate,
terminationReason,
terminationCode,
startedWithFreeTrial,
nextRetryDate,
origin,
-- subscribedInTerritory,
previousSubscriptionGlobalId,
paymentMethodId,
inFreeTrial,
subscribedInSite,
purchaseTerritory,

direct.verifiedHomeTerritory.expirationDate as direct_verifiedHomeTerritory_expirationDate,
direct.verifiedHomeTerritory.code as direct_verifiedHomeTerritory_code,
direct.subscriptionInstallment.remainingInstalments as direct_subscriptionInstallment_remainingInstallments,
direct.subscriptionInstallment.renewalCycleStartDate as direct_subscriptionInstallment_renewalCycleStartDate,
direct.campaignId as direct_campaignId,
direct.campaignCode as direct_campaignCode,
direct.minimumTermEndDate as direct_minimumTermEndDate,
direct.affiliate as direct_affiliate,
direct.paymentProvider as direct_paymentProvider,
direct.subscriptionType as direct_subscriptionType,
direct.terminationAllowed as direct_terminationAllowed,
direct.affiliateStartDate as direct_affiliateStartDate,
direct.pricePlanChangeDate as direct_pricePlanChangeDate,
direct.paymentMethodId as direct_paymentMethodId,
direct.minimunTermEndDate as direct_minimunTermEndDate,

iap.verifiedHomeTerritory.expirationDate as iap_verifiedHomeTerritory_expirationDate,
iap.verifiedHomeTerritory.code as iap_verifiedHomeTerritory_code,
iap.campaignId as iap_campaignId,
iap.campaignCode as iap_campaignCode,
iap.originalProviderPaymentReference as iap_originalProviderPaymentReference,
iap.providerUserId as iap_providerUserId,
iap.provider as iap_provider,
iap.iapSubscriptionId as iap_iapSubscriptionId,
iap.pauseDate as iap_pauseDate,
iap.pauseCode as iap_pauseCode,
iap.pauseReason as iap_pauseReason,

partner.gauthSubscriptionId as partner_gauthSubscriptionId,
partner.gauthUserId as partner_gauthUserId,
partner.partnerId as partner_partnerId,
partner.sku as partner_sku

from bolt_finint_prod.latam_silver.fi_subscriptionv2_enriched
group by all
)


-- COMMAND ----------

-- DBTITLE 1,Transactions
create or replace temporary view Transactions as
(

select

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.refundReference
  else id
end as event_id,

-- id as transaction_id,
-- invoiceid
-- st.event.refunded.refundReference as refund_id,

case
  when st.event.refunded.providerRefundReference is not null then id
end as refunded_transaction_id,

realm,
userId,
created as created_date,
st.event.occurred as event_occurred_date,
source.type as source_type,
source.reference as source_reference,

-- amountDetails.simpleAmount.minorAmount as amountDetails_simpleAmount_minorAmount,
-- amountDetails.simpleAmount.taxationCountryCode as amountDetails_simpleAmount_taxationCountryCode,

case
  when st.event.refunded.providerRefundReference is not null then coalesce(st.event.refunded.amountDetails.amountWithTax.minorAmountInclTax * -1, amountDetails.amountWithTax.minorAmountInclTax *-1)
  when st.event.chargeback.providerChargebackReference is not null then coalesce(st.event.chargeback.amountDetails.amountWithTax.minorAmountInclTax * -1, amountDetails.amountWithTax.minorAmountInclTax *-1)
  else coalesce(amountDetails.amountWithTax.minorAmountInclTax, amountDetails.simpleAmount.minorAmount)
end as charged_amount,

case
  when st.event.refunded.providerRefundReference is not null then coalesce(st.event.refunded.amountDetails.currencyCode,amountDetails.currencyCode)
  when st.event.chargeback.providerChargebackReference is not null then coalesce(st.event.chargeback.amountDetails.currencyCode,amountDetails.currencyCode)
  else amountDetails.currencyCode
end as currency,

case
  when st.event.refunded.providerRefundReference is not null then coalesce(st.event.refunded.amountDetails.amountWithTax.taxMinorAmount * -1,coalesce(amountDetails.amountWithTax.taxMinorAmount*-1,0))
  when st.event.chargeback.providerChargebackReference is not null then coalesce(st.event.chargeback.amountDetails.amountWithTax.taxMinorAmount * -1,coalesce(amountDetails.amountWithTax.taxMinorAmount*-1,0))
  else coalesce(amountDetails.amountWithTax.taxMinorAmount,0)
end as tax_amount,

case
  when st.event.refunded.providerRefundReference is not null then coalesce(st.event.refunded.amountDetails.amountWithTax.components[0].description, amountDetails.amountWithTax.components[0].description)
  when st.event.chargeback.providerChargebackReference is not null then coalesce(st.event.chargeback.amountDetails.amountWithTax.components[0].description,amountDetails.amountWithTax.components[0].description)
  else amountDetails.amountWithTax.components[0].description
end as tax_description,

case
  when st.event.refunded.providerRefundReference is not null then coalesce(st.event.refunded.amountDetails.amountWithTax.taxRateMinorUnits/100, amountDetails.amountWithTax.taxRateMinorUnits/100)
  when st.event.chargeback.providerChargebackReference is not null then coalesce(st.event.chargeback.amountDetails.amountWithTax.taxRateMinorUnits/10000, amountDetails.amountWithTax.taxRateMinorUnits/100)
  else amountDetails.amountWithTax.taxRateMinorUnits/100
end as tax_rate,

case
  when st.event.refunded.providerRefundReference is not null then UPPER(coalesce(st.event.refunded.amountDetails.amountWithTax.taxationCountryCode, amountDetails.amountWithTax.taxationCountryCode))
  when st.event.chargeback.providerChargebackReference is not null then UPPER(st.event.chargeback.amountDetails.amountWithTax.taxationCountryCode)
  else UPPER(amountDetails.amountWithTax.taxationCountryCode)
end as tax_country_code,


-- amountDetails.amountWithTax.minorAmountInclTax as amountDetails_amountWithTax_minorAmountInclTax,
-- amountDetails.amountWithTax.taxMinorAmount as amountDetails_amountWithTax_taxMinorAmount,
-- amountDetails.currencyCode as amountDetails_currencyCode,
-- amountDetails.amountWithTax.taxRateMinorUnits/10000 as amountDetails_amountWithTax_taxRate,
-- amountdetails.amountWithTax.components[0].description as tax_description,
-- amountDetails.amountWithTax.taxationCountryCode as amountDetails_amountWithTax_taxationCountryCode,
-- amountDetails.amountWithTax.taxDocumentReference as amountDetails_amountWithTax_taxDocumentReference,

case
  when st.event.pending.providerPaymentReference is not null then 'PENDING'
  when st.event.successful.providerPaymentReference is not null then 'SUCCESSFUL'
  when st.event.canceled.reason is not null then 'CANCELLED'
  when st.event.failed.providerPaymentReference is not null then 'FAILED'
  when st.event.retrying.nextretry is not null then 'RETRYING'
  when st.event.refunding.source is not null then 'REFUNDING'
  when st.event.refunded.providerRefundReference is not null then 'REFUNDED'
  when st.event.refundFailed.reason is not null then 'REFUND FAILED'
  when st.event.chargeback.providerChargebackReference is not null then 'CHARGEBACK'
  when st.event.timedOut.emptyObject is not null then 'TIMED OUT'
  when st.event.revoked.reason is not null then 'REVOKED'
  when st.event.chargebackRejected.providerChargebackReference is not null then 'CHARGEBACK REJECTED'
  when (st.event.corrected.providerPaymentReference is not null OR st.event.corrected.providerRefundReference is not null) then 'CORRECTED'
  else 'UNKNOWN'
end as event_type,

case
  when st.event.pending.providerPaymentReference is not null then st.event.pending.providerPaymentReference
  when st.event.successful.providerPaymentReference is not null then st.event.successful.providerPaymentReference
  when st.event.failed.providerPaymentReference is not null then st.event.failed.providerPaymentReference
  when st.event.refunding.providerRefundReference is not null then st.event.refunding.providerRefundReference
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.providerRefundReference
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.providerChargebackReference
  when st.event.chargebackRejected.providerChargebackReference is not null then st.event.chargebackRejected.providerChargebackReference
  when st.event.corrected.providerPaymentReference is not null then st.event.corrected.providerPaymentReference
  when st.event.corrected.providerRefundReference is not null then st.event.corrected.providerRefundReference
end as provider_Reference_id,

case
  when st.event.canceled.reason is not null then st.event.canceled.reason
  when st.event.failed.reason is not null then st.event.failed.reason 
  when st.event.retrying.reason is not null then st.event.retrying.reason
  when st.event.refundFailed.reason is not null then st.event.refundFailed.reason
  when st.event.revoked.reason is not null then st.event.revoked.reason
  when st.event.chargebackRejected.reason is not null then st.event.chargebackRejected.reason
  when st.event.timedOut.emptyObject is not null then st.event.timedOut.emptyObject
end as event_state_reason,

case
  when st.event.refunding.providerRefundReference is not null then st.event.refunding.source
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.source
end as refund_source,

replace(provider,'PAYMENT_PROVIDER_','') as payment_provider,
-- billingAddressId,

-- case
--   when type = 'PAYMENT_TRANSACTION_TYPE_INTERACTIVE' 
--     and (amountDetails.amountWithTax.minorAmountInclTax = 0 
--           OR amountDetails.amountWithTax.minorAmountInclTax is null) 
--     then 'FIRST_FREE'
--   when type = 'PAYMENT_TRANSACTION_TYPE_INTERACTIVE' and st.event.successful.providerPaymentReference is not null then 'FIRST'
--   when (st.event.refunding.providerRefundReference is not null 
--           OR st.event.refunded.providerRefundReference is not null) 
--     then 'REFUND'
--   when type = 'PAYMENT_TRANSACTION_TYPE_AUTOMATED' then 'RECURRING'
-- end as transaction_type,

replace(type, 'PAYMENT_TRANSACTION_TYPE_','') as payment_type,
paymentMethodId,
merchantAccount,
items[0].subscriptionDetails.serviceperiod.startdate as service_period_startdate,
items[0].subscriptionDetails.serviceperiod.enddate as service_period_enddate,
st.event.retrying.nextretry as next_retry_date

from bolt_finint_prod.latam_silver.fi_transactionv2_enriched
LATERAL VIEW explode_outer(stateTransitions) st as event
group by all

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Finance Views

-- COMMAND ----------

-- DBTITLE 1,Active Retail Subscriptions
create or replace temporary view Active_Retail_Subscriptions as 
(
with latest_charge as (
  select 
  t.event_id,
  t.event_type,
  t.event_state_reason,
  t.payment_provider,
  t.paymentmethodid,
  t.source_reference,
  pm.cardDetails.cardProvider as card_provider,
  pm.cardDetails.fundingSource as card_type,
  t.created_date,
  t.event_occurred_date,
  t.service_period_startdate,
  t.service_period_enddate,
  case
    when UPPER(t.Currency) in ('CLP','PYG') then coalesce(t.charged_amount, 0)
    else (coalesce(t.charged_amount, 0)) /100
  end as charged_amount

  from transactions t
  left join bolt_finint_prod.latam_silver.fi_paymentmethodv2_enriched pm on t.paymentMethodId = pm.id
  where event_type IN ('SUCCESSFUL','UNKNOWN','PENDING','FAILED', 'RETRYING')
  and charged_amount != 0
  QUALIFY ROW_NUMBER() OVER(PARTITION BY t.source_reference ORDER BY t.created_date desc) = 1
)

select
s.realm,
s.origin,
s.startedWithFreeTrial,
s.inFreeTrial,
s.subscribedInSite,
s.purchaseTerritory,
replace(s.status,'STATUS_','') as status,

case
  when s.direct_paymentProvider is not null then 'Direct'
  when s.iap_provider is not null then 'IAP'
  when s.partner_partnerId is not null then 'Partner'
end as provider_type,

coalesce(p.provider,'WEB') as provider_name,

lc.payment_provider as last_payment_provider,

s.direct_affiliate as affiliate,

replace(p.market,'TG_MARKET_','')as priceplan_market,
p.productname,
replace(p.tiertype,'TIER_TYPE_','') as tier_type,
p.campaignName,
p.internalname,
replace(p.priceplantype,'TYPE_','') as priceplantype,
replace(p.paymentPeriod,'PERIOD_','') as paymentperiod,
p.numberOfPeriodsBetweenPayments,
p.plan_price as plan_price,
p.currency as plan_currency,

s.startDate::date,
s.direct_affiliateStartDate::date,
s.nextRenewalDate::date,
s.nextRetryDate::date,
s.endDate::date,
s.cancellationDate::date,
UPPER(lc.card_provider) as card_provider,
UPPER(lc.card_type) as card_type,
lc.payment_provider,
lc.created_date::date as last_charge,
lc.event_type as last_charge_result,
lc.event_state_reason as last_result_reason,
lc.service_period_startdate::date as last_invoice_start_date,
lc.service_period_enddate::date as last_invoice_end_date,
lc.charged_amount as last_invoice_amount,
s.globalsubscriptionid,
s.previousSubscriptionGlobalId,
s.userid

from Subscriptions s
left join plans p on s.priceplanid = p.priceplanid
left join latest_charge lc on s.globalsubscriptionid = lc.source_reference

where 1=1
and s.status IN ('STATUS_ACTIVE','STATUS_CANCELED')
group by all
having provider_type in ('Direct','IAP')
)

-- COMMAND ----------

-- DBTITLE 1,Transactions Extract
create or replace temporary view Transactions_Extract as
(
select 
t.realm,
-- t.created_date::date,
t.event_occurred_date::date as transaction_date,

case 
  when t.service_period_startdate is null then t.created_date::date
  else t.service_period_startdate::date 
end as invoice_start_date,

case 
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_MONTH' then add_months(t.created_date::date, p.numberOfPeriodsBetweenPayments)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_YEAR' then add_months(t.created_date::date, 12)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_DAY' then date_add(t.created_date::date, 1)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_WEEK' then date_add(t.created_date::date, 7)
  else t.service_period_enddate::date
end as invoice_end_date,

t.source_type,
t.currency,
UPPER(t.tax_country_code) as tax_country_code,
t.event_type,
t.payment_provider,
pm.cardDetails.cardProvider,
pm.cardDetails.fundingSource,
t.payment_type,
replace(p.market,'TG_MARKET_','')as priceplan_market,
p.productname,
replace(p.tiertype,'TIER_TYPE_','') as tier_type,
p.campaignName,
p.internalname,
replace(p.priceplantype,'TYPE_','') as priceplantype,
replace(p.paymentPeriod,'PERIOD_','') as paymentperiod,
p.numberOfPeriodsBetweenPayments as period_frequency,
p.plan_price as plan_price,
p.currency as plan_currency,
t.userid,
t.event_id,
case
  when UPPER(t.Currency) in ('CLP','PYG') then coalesce(t.charged_amount, 0)
  else coalesce(t.charged_amount, 0) /100
end as charged_amount,

case
  when upper(t.Currency) in ('CLP','PYG') then coalesce(t.tax_amount, 0)
  else coalesce(t.tax_amount, 0) /100
end as tax_amount,

case
  when upper(t.Currency) in ('CLP','PYG') then coalesce(t.charged_amount, 0) - coalesce(t.tax_amount, 0)
  else (coalesce(t.charged_amount, 0) - coalesce(t.tax_amount, 0)) /100 
end as revenue_amount

from Transactions t
left join subscriptions s on t.source_reference = s.globalsubscriptionid
left join plans p on p.priceplanid = s.priceplanid
left join bolt_finint_prod.latam_silver.fi_paymentmethodv2_enriched pm on t.paymentmethodid = pm.id

where 1=1
group by all
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Financial Reports

-- COMMAND ----------

-- DBTITLE 1,Active DTC Subscribers Report
Select 
realm,
origin,
startedWithFreeTrial,
inFreeTrial,
subscribedInSite,
purchaseTerritory,
status,
provider_type,
provider_name,
last_payment_provider,
affiliate,
priceplan_market,
productname,
tier_type,
campaignName,
internalname,
priceplantype,
paymentperiod,
numberOfPeriodsBetweenPayments,
plan_price,
plan_currency,
startDate,
direct_affiliateStartDate,
nextRenewalDate,
nextRetryDate,
endDate,
cancellationDate,
card_provider,
card_type,
payment_provider,
last_charge,
last_charge_result,
last_result_reason,
last_invoice_start_date,
last_invoice_end_date,
-- last_invoice_amount,
count(globalsubscriptionid) as Subscription_count
-- globalsubscriptionid
-- previousSubscriptionGlobalId
-- userid
from Active_Retail_Subscriptions
where provider_type = 'Direct'
group by all

-- COMMAND ----------

-- DBTITLE 1,DTC Transactions Report
select 
realm,
transaction_date,
invoice_start_date,
invoice_end_date,
source_type,
currency,
tax_country_code,
event_type,
payment_provider,
cardProvider,
fundingSource,
payment_type,
priceplan_market,
productname,
tier_type,
campaignName,
internalname,
priceplantype,
paymentperiod,
period_frequency,
plan_price,
plan_currency,
count(event_id) as quantity,
sum(charged_amount) as total_charged,
sum(tax_amount) as total_tax,
sum(revenue_amount) as total_revenue
from Transactions_Extract
where transaction_date::date >= '2024-02-27'
group by all
-- and event_type in ('SUCCESSFUL','REFUNDED','CHARGEBACK')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Financial Controls

-- COMMAND ----------

-- DBTITLE 1,Duplicate Transactions
with refunds as 
(
  select 
    refunded_transaction_id,
    SUM(charged_amount) as Total_Refunded
  from transactions
  where event_type IN ('REFUNDED','CHARGEBACK')
  group by refunded_transaction_id
)

Select 
  t.userid,
  t.created_date::date,
  t.service_period_startdate::date,
  p.productname,
  p.plan_price as plan_price,
  count(distinct t.event_id) as count,
  coalesce(SUM(t.charged_amount),0) as total_charges,
  count(r.refunded_transaction_id) as refunds_count,
  coalesce(SUM(r.Total_Refunded),0) as total_refunded
from Transactions t
left join refunds r on t.event_id = r.refunded_transaction_id
left join subscriptions s on t.source_reference = s.globalsubscriptionid
left join plans p on s.priceplanid = p.priceplanid 
where 1=1
  and t.event_type = 'SUCCESSFUL'
  and created_date::date >= '2024-02-27'
group by 1, 2 ,3 ,4,5
HAVING count >1
and total_refunded = 0

-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions
with dupe_subs as (
select
userid,
count(distinct globalsubscriptionid) as subscription_count
from subscriptions
where direct_paymentProvider is not null
and status in ('STATUS_ACTIVE','STATUS_CANCELED')
group by 1
having subscription_count > 1
)

select 
d.userid,
s.globalsubscriptionid,
status
from subscriptions s 
inner join dupe_subs d on s.userid = d.userid 
where status in ('STATUS_ACTIVE','STATUS_CANCELED')

-- COMMAND ----------

-- DBTITLE 1,Unbilled Subscriptions
select
  userid,
  globalsubscriptionid,
  subscribedinsite,
  Provider_type,
  provider_name,
  status,
  startdate,
  nextrenewaldate,
  nextretrydate,
  last_charge,
  last_charge_result,
  last_result_reason,
  last_invoice_start_date,
  last_invoice_end_date,
  last_invoice_amount,
  priceplan_market
from Active_retail_subscriptions
where 1=1
  and nextrenewaldate <= current_date
  and nextretrydate is null
group by all
Having provider_type = 'Direct'



-- COMMAND ----------

-- DBTITLE 1,Next Renewal Date Validation
select
  userid,
  globalsubscriptionid,
  Provider_type,
  provider_name,
  subscribedinsite,
  status,
  startdate,
  nextretrydate,
  tier_type,
  productname,
  paymentperiod,
  numberOfPeriodsBetweenPayments,
  nextrenewaldate,

  case 
    when paymentperiod = 'MONTH' and months_between(nextrenewaldate, current_date()) <= numberOfPeriodsBetweenPayments THEN 'as expected'
    when paymentperiod = 'YEAR' and months_between(nextrenewaldate, current_date()) <= (numberOfPeriodsBetweenPayments * 12) THEN 'as expected'
    else 'not expected'
  end as next_renewal_check,

  last_charge,
  last_charge_result,
  last_result_reason,
  last_invoice_start_date,
  last_invoice_end_date,
  priceplan_market
from Active_retail_subscriptions
where 1=1
and paymentperiod in ('MONTH','YEAR')

group by all
Having provider_type = 'Direct'
and next_renewal_check = 'not expected'

-- COMMAND ----------

-- DBTITLE 1,Billed Amount Check
Select *
from transactions_extract
where event_type IN ('SUCESSFUL','FAILED','RETRYING')
and transaction_Date >= '2024-02-26'
and invoice_start_date >= '2024-02-01'
group by all
Having charged_amount - plan_price > 0

-- COMMAND ----------

-- DBTITLE 1,Renewals Check
select *
from transactions
where created_date >= '2024-02-27T09:00:00.000+00:00'
and payment_type = 'AUTOMATED'
