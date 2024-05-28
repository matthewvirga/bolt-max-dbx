-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Kafka Main Views

-- COMMAND ----------

-- DBTITLE 1,Plans
create or replace temporary view Plans as
(
select
pp.realm,
  case
    when marketid = 'TG_MARKET_UNITED_STATES' then 'AMER'
    when marketid in (
      'TG_MARKET_BOLIVIA',
      'TG_MARKET_PARAGUAY',
      'TG_MARKET_COSTA_RICA',
      'TG_MARKET_CHILE',
      'TG_MARKET_DOMINICA',
      'TG_MARKET_GUATEMALA',
      'TG_MARKET_BARBADOS',
      'TG_MARKET_ECUADOR',
      'TG_MARKET_MEXICO',
      'TG_MARKET_TURKS_AND_CAICOS',
      'TG_MARKET_DOMINICAN_REPUBLIC',
      'TG_MARKET_COLOMBIA',
      'TG_MARKET_ST_KITTS_AND_NEVIS',
      'TG_MARKET_BELIZE',
      'TG_MARKET_EL_SALVADOR',
      'TG_MARKET_HAITI',
      'TG_MARKET_GRENADA',
      'TG_MARKET_PANAMA',
      'TG_MARKET_BRITISH_VIRGIN_ISLANDS',
      'TG_MARKET_JAMAICA',
      'TG_MARKET_NICARAGUA',
      'TG_MARKET_GUATEMALA',
      'TG_MARKET_BAHAMAS',
      'TG_MARKET_ARGENTINA',
      'TG_MARKET_BRAZIL',
      'TG_MARKET_TRINIDAD_AND_TOBAGO',
      'TG_MARKET_ST_VINCENT_AND_GRENADINES',
      'TG_MARKET_CURACAO',
      'TG_MARKET_PERU',
      'TG_MARKET_URUGUAY',
      'TG_MARKET_ST_LUCIA',
      'TG_MARKET_MEXICO',
      'TG_MARKET_ARUBA',
      'TG_MARKET_VENEZUELA',
      'TG_MARKET_GUYANA',
      'TG_MARKET_HONDURAS',
      'TG_MARKET_ANTIGUA_AND_BARBUDA',
      'TG_MARKET_CAYMAN_ISLANDS',
      'TG_MARKET_SURINAME',
      'TG_MARKET_PERU',
      'TG_MARKET_EL_SALVADOR',
      'TG_MARKET_BRAZIL',
      'TG_MARKET_ANGUILLA',
      'TG_MARKET_COSTA_RICA',
      'TG_MARKET_ARGENTINA',
      'TG_MARKET_HONDURAS',
      'TG_MARKET_CHILE',
      'TG_MARKET_MONTSERRAT'
    ) then 'LATAM'
    else 'EMEA'
  end as Region,
  case 
    when marketid IN ('TG_MARKET_NETHERLANDS','TG_MARKET_POLAND','TG_MARKET_FRANCE','TG_MARKET_BELGIUM') then 'WAVE 2'
    else null
  end as launch_wave,
  marketId as market,
  pp.id as priceplanid,
  pr.addOnIds [0] as addOnIds,
  pr.mainProductIds [0] as mainProductIds,
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
baseSubscriptionId,
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
-- direct.minimunTermEndDate as direct_minimunTermEndDate,

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

from bolt_finint_prod.beam_emea_silver.fi_subscriptionv2_enriched
group by all
)


-- COMMAND ----------

-- DBTITLE 1,Transactions
create or replace temporary view Transactions as
(

select

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.refundReference
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.providerChargebackReference
  else id
end as event_id,

-- id as transaction_id,
-- invoiceid
-- st.event.refunded.refundReference as refund_id,

case
  when st.event.refunding.providerRefundReference is not null then id
  when st.event.refunded.providerRefundReference is not null then id
  when st.event.chargeback.providerChargebackReference is not null then id
end as original_transaction_id,

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
  when (st.event.failed.providerPaymentReference is not null OR st.event.failed.reason is not null) then 'FAILED'
  when (st.event.retrying.nextretry is not null OR st.event.retrying.reason is not null) then 'RETRYING'
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


from bolt_finint_prod.beam_emea_silver.fi_transactionv2_enriched
LATERAL VIEW explode_outer(stateTransitions) st as event
group by all

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Finance Views

-- COMMAND ----------

-- DBTITLE 1,Active Subscriptions
create or replace temporary view Active_Subscriptions as 
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
  left join bolt_finint_prod.beam_emea_silver.fi_paymentmethodv2_enriched pm on t.paymentMethodId = pm.id
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
p.launch_wave,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.productname,
replace(p.tiertype,'TIER_TYPE_','') as tier_type,
p.campaignName,
p.internalname,
replace(p.priceplantype,'TYPE_','') as priceplantype,
replace(p.paymentPeriod,'PERIOD_','') as paymentperiod,
p.numberOfPeriodsBetweenPayments,
p.plan_price as plan_price,
p.currency as plan_currency,

s.startDate,
s.direct_affiliateStartDate,
s.nextRenewalDate,
s.nextRetryDate,
s.endDate,
s.cancellationDate,
UPPER(lc.card_provider) as card_provider,
UPPER(lc.card_type) as card_type,
lc.payment_provider,
lc.created_date as last_charge,
lc.event_type as last_charge_result,
lc.event_state_reason as last_result_reason,
lc.service_period_startdate as last_invoice_start_date,
lc.service_period_enddate as last_invoice_end_date,
lc.charged_amount as last_invoice_amount,
s.globalsubscriptionid,
s.baseSubscriptionId,
s.previousSubscriptionGlobalId,
s.userid

from Subscriptions s
left join plans p on s.priceplanid = p.priceplanid
left join latest_charge lc on s.globalsubscriptionid = lc.source_reference

where 1=1
and s.status IN ('STATUS_ACTIVE','STATUS_CANCELED')
group by all
having provider_type in ('Direct','IAP','Partner')
-- and priceplan_market not in (
--   'NETHERLANDS',
--   'POLAND',
--   'FRANCE',
--   'BELGIUM'
-- )
)

-- COMMAND ----------

-- DBTITLE 1,Transactions Extract
create or replace temporary view Transactions_Extract as
(
select 
t.realm,
t.created_date,
t.event_occurred_date as transaction_date,

case 
  when t.service_period_startdate is null then t.created_date
  else t.service_period_startdate
end as invoice_start_date,

case 
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_MONTH' then add_months(t.created_date, p.numberOfPeriodsBetweenPayments)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_YEAR' then add_months(t.created_date, 12)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_DAY' then date_add(t.created_date, 1)
  when t.service_period_enddate is null and p.paymentPeriod = 'PERIOD_WEEK' then date_add(t.created_date, 7)
  else t.service_period_enddate
end as invoice_end_date,

t.source_type,
t.currency,
UPPER(t.tax_country_code) as tax_country_code,
t.event_type,
t.payment_provider,
s.direct_affiliate,
pm.cardDetails.cardProvider,
pm.cardDetails.fundingSource,
t.payment_type,
replace(p.market,'TG_MARKET_','')as priceplan_market,
p.launch_wave,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.productname,
replace(p.tiertype,'TIER_TYPE_','') as tier_type,
p.campaignName,
p.internalname,
replace(p.priceplantype,'TYPE_','') as priceplantype,
replace(p.paymentPeriod,'PERIOD_','') as paymentperiod,
p.numberOfPeriodsBetweenPayments as period_frequency,
p.plan_price as plan_price,
p.currency as plan_currency,
p.currencyDecimalPoints,
t.userid,
s.globalsubscriptionid,
s.basesubscriptionid,
s.direct_affiliate as affiliate,
t.event_id,
t.original_transaction_id,
t.provider_Reference_id,
t.merchantaccount,

round(coalesce(t.charged_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as charged_amount,
round(coalesce(t.tax_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as tax_amount,
round((coalesce(t.charged_amount, 0) - coalesce(t.tax_amount, 0)) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as revenue_amount

from Transactions t
left join subscriptions s on t.source_reference = s.globalsubscriptionid
left join plans p on p.priceplanid = s.priceplanid
left join bolt_finint_prod.beam_emea_silver.fi_paymentmethodv2_enriched pm on t.paymentmethodid = pm.id

where 1=1
-- and t.created_date >= '2024-05-20T23:00:00.000+00:00'
group by all
-- having priceplan_market not in (
--   'NETHERLANDS',
--   'POLAND',
--   'FRANCE',
--   'BELGIUM'
-- )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Launch Controls

-- COMMAND ----------

-- DBTITLE 1,Recurring Job Check
select 
DATE_FORMAT(t.created_date, 'yyyy-MM-dd') AS date_ts,
DATE_FORMAT(t.created_date, 'HH') AS hour,
t.event_type,
t.product_type,
-- priceplan_market,
count(distinct t.event_id)
from transactions_extract t
left join subscriptions s on t.globalsubscriptionid = s.globalsubscriptionid
where t.created_date >= '2024-05-21T01:00:00.000+00:00'
-- and s.startdate <= '2024-05-20T23:00:00.000+00:00'
-- and invoice_start_date <= '2024-05-20T23:00:00.000+00:00'
and t.payment_provider in (
  'PAYPAL',
  'ADYEN'
)
and t.payment_type = 'AUTOMATED'
and t.launch_wave is null
and t.event_type in (
  'SUCCESSFUL',
  'FAILED',
  'RETRYING'
)
group by all
order by date_ts desc, hour desc;

-- select 
-- DATE_FORMAT(t.created_date, 'yyyy-MM-dd') AS date,
-- DATE_FORMAT(t.created_date, 'HH') AS hour,
-- t.event_type,
-- t.product_type,
-- t.payment_type,
-- t.userid,
-- t.event_id
-- from transactions_extract t
-- left join subscriptions s on t.globalsubscriptionid = s.globalsubscriptionid
-- where t.created_date >= '2024-05-21T01:00:00.000+00:00'
-- and t.payment_provider in (
--   'PAYPAL',
--   'ADYEN'
-- )
-- and t.payment_type = 'AUTOMATED'
-- and t.launch_wave is null
-- and t.event_type in (
--   'SUCCESSFUL',
--   'FAILED',
--   'RETRYING'
-- )
-- -- and product_type = 'ADD_ON'
-- group by all
-- order by date desc, hour desc;

-- COMMAND ----------

-- DBTITLE 1,Successful Volume Check (New + Recurring)
select
transaction_date::date as date,
event_type,
launch_wave,
count(event_id) as quantity
from transactions_extract
where transaction_date >= '2024-05-20T23:00:00.000+00:00'
and event_type IN ('SUCCESSFUL','REFUNDED')
group by all

-- COMMAND ----------

-- DBTITLE 1,DTC Users in Grace
select 
-- startdate::date as start_date,
nextrenewaldate::date,
count(globalsubscriptionid)
from active_subscriptions
where nextrenewaldate <= current_date()
and status = 'ACTIVE'
and provider_type = 'Direct'
group by all
order by nextrenewaldate desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Add-On Controls

-- COMMAND ----------

-- DBTITLE 1,Add-On Base Sub ID Check
select * from active_subscriptions
where product_type = 'ADD_ON'
and provider_type = 'DTC'
and basesubscriptionID is null

-- COMMAND ----------

-- DBTITLE 1,Active Add-On Terminated Base
with base as 
(
select * from active_subscriptions
where product_type = 'MAIN'
and status = 'TERMINATED'
)
,
add_ons as (
select * from active_subscriptions
where product_type = 'ADD_ON'
and status IN ('ACTIVE','CANCELED')
)

Select *
from add_ons a
inner join base b on a.basesubscriptionid = b.globalsubscriptionid

-- COMMAND ----------

-- DBTITLE 1,MVPD Add-On Refund
with mvpd_base as 
(
select 
product_type,
provider_type,
globalsubscriptionid,
basesubscriptionid 
from active_subscriptions
where product_type = 'MAIN'
and provider_type = 'Partner'
)
,
add_on_refunds as (
  select * from transactions_extract
  where product_type = 'ADD_ON'
  and event_type IN ('REFUNDED', 'CHARGEBACK')
)

select 
a.*
from add_on_refunds a
inner join mvpd_base b on a.basesubscriptionid = b.globalsubscriptionid
group by all
having (plan_price + charged_amount) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Financial Controls

-- COMMAND ----------

-- DBTITLE 1,Duplicate Transactions
WITH refunds AS (
    SELECT 
        original_transaction_id,
        SUM(charged_amount) AS total_refunded
    FROM transactions
    WHERE event_type IN ('REFUNDED', 'CHARGEBACK')
    GROUP BY original_transaction_id
)

SELECT 
    t.userid,
    t.source_type,
    t.created_date::date,
    t.service_period_startdate::date,
    p.producttype,
    p.productname,
    p.plan_price,
    COUNT(DISTINCT t.event_id) AS count,
    round(COALESCE(SUM(t.charged_amount), 0),2) AS charged_amount,
    COUNT(r.original_transaction_id) AS refunds_count,
    round(COALESCE(SUM(r.total_refunded), 0),2) AS total_refunded
FROM transactions t
LEFT JOIN refunds r ON t.event_id = r.original_transaction_id
LEFT JOIN subscriptions s ON t.source_reference = s.globalsubscriptionid
LEFT JOIN plans p ON s.priceplanid = p.priceplanid 
WHERE t.event_type = 'SUCCESSFUL'
    AND t.created_date >= '2024-05-20T23:00:00.000+00:00'
GROUP BY ALL
HAVING count > 1
    AND total_refunded = 0;

-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions
select
userid,
count(distinct globalsubscriptionid) as subscription_count
from active_subscriptions
where provider_type = 'Direct'
and status in ('STATUS_ACTIVE','STATUS_CANCELED')
and product_type = 'MAIN'
group by 1
having subscription_count > 1

-- COMMAND ----------

-- DBTITLE 1,Unbilled Subscriptions
select
  s.userid,
  u.record.user.email as user_email,
  s.globalsubscriptionid,
  s.subscribedinsite,
  s.Provider_type,
  s.provider_name,
  s.product_type,
  s.productname,
  s.campaignname,
  s.internalname,
  s.status,
  s.startdate::date,
  s.nextrenewaldate::date,
  s.nextretrydate::date,
  s.plan_price,
  s.last_charge::date,
  s.last_charge_result,
  s.last_result_reason,
  s.last_invoice_start_date::date,
  s.last_invoice_end_date::date,
  s.last_invoice_amount,
  s.priceplan_market,
  s.launch_wave
from Active_subscriptions s
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on s.userid = u.record.user.userid
where 1=1
  -- and launch_wave is null
  and nextrenewaldate <= current_date()
  and nextretrydate is null
  and plan_price > 0
group by all
Having provider_type = 'Direct'

-- COMMAND ----------

-- DBTITLE 1,Next Renewal Date Validation
select
  s.userid,
  u.record.user.email as user_email,
  s.globalsubscriptionid,
  s.Provider_type,
  s.provider_name,
  s.subscribedinsite,
  s.status,
  s.startdate,
  s.nextretrydate,
  s.tier_type,
  s.productname,
  s.paymentperiod,
  s.numberOfPeriodsBetweenPayments,
  s.nextrenewaldate,

  case 
    when s.paymentperiod = 'MONTH' and months_between(s.nextrenewaldate, current_date()) <= s.numberOfPeriodsBetweenPayments THEN 'as expected'
    when s.paymentperiod = 'YEAR' and months_between(s.nextrenewaldate, current_date()) <= (s.numberOfPeriodsBetweenPayments * 12) THEN 'as expected'
    else 'not expected'
  end as next_renewal_check,

  s.last_charge,
  s.last_charge_result,
  s.last_result_reason,
  s.last_invoice_start_date,
  s.last_invoice_end_date,
  s.priceplan_market
from Active_subscriptions s
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on s.userid = u.record.user.userid
where 1=1
and paymentperiod in ('MONTH','YEAR')
and launch_wave is null
group by all
Having provider_type = 'Direct'
and next_renewal_check = 'not expected'

-- COMMAND ----------

-- DBTITLE 1,Billed Amount Check
WITH refunds AS (
    SELECT 
        original_transaction_id,
          coalesce(SUM(charged_amount),0) AS total_refunded
    FROM transactions_extract
    WHERE event_type IN ('REFUNDED', 'CHARGEBACK')
    GROUP BY original_transaction_id
)

Select t.*, 
r.original_transaction_id,
coalesce(r.total_refunded,0) as total_refunded
from transactions_extract t
left join refunds r on t.event_id = r.original_transaction_id
where 1=1
and event_type IN ('SUCCESSFUL','FAILED','RETRYING')
and created_Date >= '2024-05-20T23:00:00.000+00:00'
group by all
Having charged_amount - plan_price > 0
and total_refunded = 0


-- COMMAND ----------

-- DBTITLE 1,Wave 2 Billing in Bolt
with hurley_export_trans as (
  select
    explode(record.paymentTransactionIds) as id_mappings,
    id_mappings.legacyId as legacy_transaction_id,
    id_mappings.boltId as bolt_transaction_id,
    record.boltUserId as bolt_user_id,
    record.legacyUserId as legacy_user_id,
    updatedAt
  from
    bolt_dcp_prod.any_emea_bronze.raw_s2s_subscription_legacy_mapping_entities
),
payment_tx as (
  select
    record.transaction.id as transaction_id,
    record.transaction.amountDetails.amountWithTax.taxationCountryCode as country,
    record.transaction.realm as realm,
    record.transaction.source.reference as subscription_id,
    record.transaction.userId as raw_bolt_user_id
  from
    bolt_dcp_prod.any_emea_bronze.raw_s2s_payment_transaction_entities
  where
    (
      record.transaction.amountDetails.simpleAmount is not null
      or record.transaction.amountDetails.amountWithTax.minorAmountInclTax is not null
    )
    and createdAt >= '2024-05-21T00:00:00Z'
),
temp_table as (
  select
    m.legacy_transaction_id,
    m.bolt_transaction_id,
    m.bolt_user_id,
    m.legacy_user_id,
    m.updatedAt
  from
    hurley_export_trans m
    join bolt_finint_prod.any_emea_bronze.migration_state_entities s on m.bolt_user_id = s.record.migrationState.boltUserAccountId
  where
    s.record.migrationState.entityType = 'ENTITY_TYPE_COMMERCE'
),
main as (
  select
    m.bolt_user_id,
    m.legacy_user_id,
    m.legacy_transaction_id,
    m.bolt_transaction_id,
    t.transaction_id,
    t.country as country,
    t.subscription_id,
    t.raw_bolt_user_id,
    m.updatedAt
  from
    payment_tx t
    left join temp_table m on t.transaction_id = m.bolt_transaction_id
)
select
  source,
  count(distinct transaction_id) as number_of_transactions,
  --transaction_id,
  country -- bolt_transaction_id,
  -- legacy_transaction_id,
  -- bolt_user_id,
  -- legacy_user_id,
  -- subscription_id,
  -- raw_bolt_user_id,
  -- updatedAt
from
  (
    select
      'BOLT' as source,
      transaction_id,
      country -- bolt_transaction_id,
      -- legacy_transaction_id,
      -- bolt_user_id,
      -- legacy_user_id,
      -- subscription_id,
      -- raw_bolt_user_id,
      -- updatedAt
    from
      main
    where
      legacy_transaction_id is null
    UNION ALL
    select
      'HBO_MAX' as source,
      transaction_id,
      country -- bolt_transaction_id,
      -- legacy_transaction_id,
      -- bolt_user_id,
      -- legacy_user_id,
      -- subscription_id,
      -- raw_bolt_user_id,
      -- updatedAt
    from
      main
    where
      legacy_transaction_id is not null
  )
where
  UPPER(country) IN ('NL', 'PL')
  and UPPER(source) != 'HBO_MAX'
group by
  source,
  country
order by
  source desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Migration Validation and UAT

-- COMMAND ----------

-- DBTITLE 1,EMEA Commerce Migration Stats
with hurley_subs_in_raw AS (
   SELECT
      record.subscription.globalSubscriptionId as globalSubscriptionId, record.subscription.userId, record.subscription.status as status
   from bolt_dcp_prod.beam_emea_bronze.raw_s2s_subscription_entities
   where record.subscription.direct is not null
   and record.subscription.subscribedInSite = 'HBO_MAX'
),
subscription_mapping AS (
   SELECT explode(record.monetizationIds.subscriptionIds) as sub_id, record.legacyUserId as legacyUserId
   FROM bolt_dcp_prod.any_emea_bronze.raw_s2s_subscription_legacy_mapping_entities
),
all_dtc_sub_ids_in_mapping AS (
   SELECT
      *
   FROM subscription_mapping sm
   where sub_id.legacyId like '%LEGACYHBO_%'
),
hurley_export as
(
    select record
    from bolt_finint_prod.any_emea_bronze.maas_observation_commerce_dtc_entities
), 
bolt_import as
(
    select record
    from bolt_finint_prod.any_emea_bronze.migration_state_entities
    where record.migrationState.entityType = 'ENTITY_TYPE_COMMERCE'
),
user_legacy_mapping as
(
    select record
    from bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_legacy_mapping_entities
),
user_legacy_mapping_in_users as 
(
   select user_mapping.record as record, user_mapping.record.userLegacyMapping.legacyUserId as legacyUserId, user_mapping.record.userLegacyMapping.userId as boltUserId
   from bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_legacy_mapping_entities user_mapping
   join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities users on
   user_mapping.record.userLegacyMapping.userId = users.record.user.userId
),
user_legacy_mapping_tombstoned as 

(
   select user_mapping.record.userLegacyMapping.legacyUserId as legacyUserId, user_mapping.record.userLegacyMapping.userId as boltUserId
   from bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_legacy_mapping_entities user_mapping
   join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities users on
   user_mapping.record.userLegacyMapping.userId = users.messageBrokerKey
   and users.record is null
),
hurley_export_missing_from_user_mappings as
(
    select record
    from hurley_export
    where record.commerceDtcObservation.legacyUserAccountId not in (
        select record.userLegacyMapping.legacyUserId
        from user_legacy_mapping
    )
),
hurley_export_missing_from_user_mappings_and_users as
(
    select record
    from hurley_export
    where record.commerceDtcObservation.legacyUserAccountId not in (
        select legacyUserId
        from user_legacy_mapping_in_users
    )
),
hurley_export_in_user_legacy_mapping_tombstoned as
(
    select record
    from hurley_export
    where record.commerceDtcObservation.legacyUserAccountId in (
        select legacyUserId
        from user_legacy_mapping_tombstoned
    )
),
hurley_export_missing_from_import_not_users as
(
    select record
    from hurley_export
    where record.commerceDtcObservation.legacyUserAccountId not in (
        select record.migrationState.migrationMetadata.legacyEntityId
        from bolt_import
    )
    and record.commerceDtcObservation.legacyUserAccountId in (
        select record.userLegacyMapping.legacyUserId
        from user_legacy_mapping
    )
),
hurley_export_in_import_but_failed as
(
    select record
    from hurley_export
    where record.commerceDtcObservation.legacyUserAccountId in (
        select record.migrationState.migrationMetadata.legacyEntityId
        from bolt_import
        where record.migrationState.boltImportResult.status != 'OK'
    )
),
hurley_export_subs as
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export
),
hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw as (
    select hurley_export_subs.subscription.id as legacySubscriptionId
    ,hurley_export_subs.subscription.userId as legacyUserId
    ,hurley_subs_in_raw.globalSubscriptionId as globalSubscriptionId
    ,hurley_subs_in_raw.status as raw_status
    ,hurley_export_subs.status as status
    from hurley_export_subs
    join all_dtc_sub_ids_in_mapping on
    hurley_export_subs.subscription.id = all_dtc_sub_ids_in_mapping.sub_id.legacyId
    join hurley_subs_in_raw on
    all_dtc_sub_ids_in_mapping.sub_id.boltId = hurley_subs_in_raw.globalSubscriptionId
    where hurley_export_subs.subscription.userId in (
            select record.userLegacyMapping.legacyUserId as userId
            from user_legacy_mapping_in_users
    )
),
hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw_and_imported_ok as (
    select *
    from hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw 
    where legacyUserId in (
            select record.migrationState.migrationMetadata.legacyEntityId as userId
            from bolt_import
            where record.migrationState.boltImportResult.status = 'OK'
    )
),
hurley_export_subs_missing_from_import_not_users as
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export_missing_from_import_not_users
),
hurley_export_subs_in_import_but_failed as
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export_in_import_but_failed
),
hurley_export_subs_missing_from_user_mappings as
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export_missing_from_user_mappings
),
hurley_export_subs_missing_from_user_mappings_and_users as
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export_missing_from_user_mappings_and_users
),
hurley_export_subs_in_user_legacy_mapping_tombstoned as 
(
    select explode(record.commerceDtcObservation.commerceDtc.subscription) as subscription, subscription.status as status, subscription.userId as legacyUserId
    from hurley_export_in_user_legacy_mapping_tombstoned
),
hurley_export_subs_in_mapping as
(
      select *
      from hurley_export_subs 
      where subscription.id in (
        select sub_id.legacyId 
        from all_dtc_sub_ids_in_mapping
      )
),
all_dtc_sub_ids_in_raw AS (
   SELECT
      sub_id.legacyId as legacyId_in_map, sub_id.boltId as boltId_in_map, subs.record.subscription.globalSubscriptionId as boltId_in_raw,  
      subs.record.subscription.status as status, subs.record.subscription.direct as is_dtc, legacyUserId
   FROM all_dtc_sub_ids_in_mapping
   JOIN bolt_dcp_prod.beam_emea_bronze.raw_s2s_subscription_entities as subs
   ON sub_id.boltId = subs.record.subscription.globalSubscriptionId  
),
hurley_export_subs_in_raw_but_not_in_users_or_user_mappings_or_sub_mappings AS (
  select * 
  from hurley_subs_in_raw 
  where globalSubscriptionId not in (
    select globalSubscriptionId from hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw
  )
),
hurley_export_subs_in_raw_but_not_in_users_or_user_mappings AS (
  select * from hurley_export_subs_in_raw_but_not_in_users_or_user_mappings_or_sub_mappings s1
  join all_dtc_sub_ids_in_mapping s2
  on s1.globalSubscriptionId = s2.sub_id.boltId
),
hurley_export_subs_in_raw_but_missing_from_user_mappings AS ( 
  select s1.subscription.id, s1.subscription.userId as legacyUserId, globalSubscriptionId, userId, s1.status 
  from hurley_export_subs_missing_from_user_mappings s1
  join hurley_export_subs_in_raw_but_not_in_users_or_user_mappings s2
  on s1.subscription.id = s2.sub_id.legacyId
),
hurley_export_subs_in_raw_but_user_tombstoned AS ( 
  select s1.subscription.id, s1.subscription.userId as legacyUserId, globalSubscriptionId, userId, s1.status 
  from hurley_export_subs_in_user_legacy_mapping_tombstoned s1
  join hurley_export_subs_in_raw_but_not_in_users_or_user_mappings s2
  on s1.subscription.id = s2.sub_id.legacyId
),
hurley_export_subs_in_import_but_failed_and_in_raw AS ( 
  select s1.* from hurley_export_subs_in_import_but_failed s1
  join all_dtc_sub_ids_in_raw s2
  on  s1.subscription.id = s2.legacyId_in_map
),
hurley_export_subs_missing_from_import_not_users_but_in_raw AS ( 
  select s1.* from hurley_export_subs_missing_from_import_not_users s1
  join all_dtc_sub_ids_in_raw s2
  on  s1.subscription.id = s2.legacyId_in_map
),
hurley_export_subs_left_join_bucket AS ( 
  select s1.*, s2.legacyUserId as other_user_2 
  from hurley_export_subs s1
  left join hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw_and_imported_ok  s2
  on s1.subscription.id =  s2.legacySubscriptionId 
  where s1.legacyUserId in (
        select legacyUserId
        from user_legacy_mapping_in_users
  )
),
hurley_export_subs_x_bucket AS ( 
  select * from hurley_export_subs_left_join_bucket
  where other_user_2 is null 
),

all_final_results AS (
    select
        'total HBO Max subs in export' as bucket,
        count(1) as total_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 0 ELSE 1 END) AS non_terminated_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 1 ELSE 0 END) AS terminated_count,
        count(distinct legacyUserId) as distinct_legacy_user_ids
    from
         hurley_export_subs
    UNION ALL
    select 
       'total HBO Max subs from export migrated end to end' as bucket,
       count(1) as total_count, 
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 0 ELSE 1 END) AS non_terminated_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 1 ELSE 0 END) AS terminated_count,
       count(distinct legacyUserId) as distinct_legacy_user_ids
    from 
        hurley_export_subs_in_users_and_user_mappings_and_sub_mapping_and_in_raw_and_imported_ok

    UNION ALL
    select 
        'total HBO Max subs from export not migrated for user reasons' as bucket,
        count(1) as total_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 0 ELSE 1 END) AS non_terminated_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 1 ELSE 0 END) AS terminated_count,
        count(distinct legacyUserId) as distinct_legacy_user_ids
    from
        hurley_export_subs_missing_from_user_mappings_and_users
    UNION ALL
    select 
        'total HBO Max subs from export not migrated for other reasons' as bucket,
        count(1) as total_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 0 ELSE 1 END) AS non_terminated_count,
        SUM(CASE status WHEN 'STATUS_TERMINATED' THEN 1 ELSE 0 END) AS terminated_count,
        count(distinct legacyUserId) as distinct_legacy_user_ids
    from
         hurley_export_subs_x_bucket
)
select * from all_final_results


-- COMMAND ----------

-- DBTITLE 1,Test Transactions
WITH refunds AS (
    SELECT 
        original_transaction_id,
        SUM(charged_amount) AS total_refunded
    FROM transactions_extract
    WHERE event_type IN ('REFUNDED', 'CHARGEBACK')
    GROUP BY original_transaction_id
)

select
t.*,
coalesce(r.total_refunded,0) as total_refunded,
u.record.user.email,
u.record.user.migrationStatus,
s.status as subscription_status
from transactions_extract t
left join subscriptions s on t.globalsubscriptionid = s.globalsubscriptionid
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on t.userid = u.record.user.userid
left join refunds r on t.event_id = r.original_transaction_id
  where event_type IN (
    'SUCCESSFUL'
  )
  and s.startdate::date <= '2024-05-10'
  -- and u.record.user.migrationStatus is null
  and UPPER(productname) not like '%LEGACY%'
group by all
Having (charged_amount + total_refunded) > 0
