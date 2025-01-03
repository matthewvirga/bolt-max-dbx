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
  marketId as market,
    case 
    when marketid IN ('TG_MARKET_NETHERLANDS','TG_MARKET_POLAND','TG_MARKET_FRANCE','TG_MARKET_BELGIUM') then 'WAVE 2'
    else null
  end as launch_wave,
  pp.id as priceplanid,
  pr.addOnIds [0] as addOnIds,
  pr.mainProductIds [0] as mainProductIds,
  pr.bundlingMemberIds [0] as bundlingMemberIds,
  pr.contractId,
  pr.bundle as bundle,
  pr.fulfillerPartnerId,
  pr.fulfillerPartnerSku,
  pr.productType as producttype,
  pr.businessType,
  pr.businessPlatform,
  pr.businessBrand,
  pr.revenueType,
  pr.businessCase,
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
  pp.currency as currency,
  pr.capabilitiesProvided
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

from bolt_finint_prod.silver.fi_subscriptionv2_enriched
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

replace(type, 'PAYMENT_TRANSACTION_TYPE_','') as payment_type,
paymentMethodId,
merchantAccount,
items[0].subscriptionDetails.serviceperiod.startdate as service_period_startdate,
items[0].subscriptionDetails.serviceperiod.enddate as service_period_enddate,
st.event.retrying.nextretry as next_retry_date


from bolt_finint_prod.silver.fi_transactionv2_enriched
LATERAL VIEW explode_outer(stateTransitions) st as event
group by all

)

-- COMMAND ----------

-- DBTITLE 1,Test Users
create or replace temporary view Test_Users As
(
  select record.user.*
from bolt_dcp_prod.bronze.raw_s2s_user_entities
where upper(record.user.email) LIKE '%H9ZKA4D71M%'
OR record.user.testUser
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Finance Views

-- COMMAND ----------

-- DBTITLE 1,Active Retail Subscriptions
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
  where event_type IN ('SUCCESSFUL','PENDING','FAILED', 'RETRYING')
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

p.priceplanid,
replace(p.market,'TG_MARKET_','')as priceplan_market,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.bundle as is_bundle,
p.businessType,
p.businessPlatform,
p.businessBrand,
p.revenueType,
p.businessCase,
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

p.contractId,
p.fulfillerPartnerId,
p.fulfillerPartnerSku,
s.globalsubscriptionid,
s.baseSubscriptionId,
s.previousSubscriptionGlobalId,
s.userid,
s.partner_gauthSubscriptionId,
s.partner_gauthUserId,
s.partner_partnerId,
s.partner_sku

from Subscriptions s
left join plans p on s.priceplanid = p.priceplanid
left join latest_charge lc on s.globalsubscriptionid = lc.source_reference

where 1=1
  -- and p.bundle
and s.status IN ('STATUS_ACTIVE','STATUS_CANCELED')
group by all
-- having provider_type in ('Direct','IAP')
)

-- COMMAND ----------

-- DBTITLE 1,All Subscriptions
create or replace temporary view All_Subscriptions as 
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
  where event_type IN ('SUCCESSFUL','PENDING','FAILED', 'RETRYING')
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

p.priceplanid,
replace(p.market,'TG_MARKET_','')as priceplan_market,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.bundle as is_bundle,
p.businessType,
p.businessPlatform,
p.businessBrand,
p.revenueType,
p.businessCase,
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

p.contractId,
p.fulfillerPartnerId,
p.fulfillerPartnerSku,
s.globalsubscriptionid,
s.baseSubscriptionId,
s.previousSubscriptionGlobalId,
s.userid,
s.partner_gauthSubscriptionId,
s.partner_gauthUserId,
s.partner_partnerId,
s.partner_sku

from Subscriptions s
left join plans p on s.priceplanid = p.priceplanid
left join latest_charge lc on s.globalsubscriptionid = lc.source_reference

where 1=1
group by all

)

-- COMMAND ----------

-- DBTITLE 1,Transactions Extract
create or replace temporary view Transactions_Extract as
(
select 
t.realm,
t.created_date::date,
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
s.direct_affiliate,
pm.cardDetails.cardProvider,
pm.cardDetails.fundingSource,
t.payment_type,
p.priceplanid,
replace(p.market,'TG_MARKET_','')as priceplan_market,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.productname,
p.bundle as is_bundle,
p.businessType,
p.businessPlatform,
p.businessBrand,
p.revenueType,
p.businessCase,
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
t.event_id,
t.original_transaction_id,
t.provider_Reference_id,

round(coalesce(t.charged_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as charged_amount,
round(coalesce(t.tax_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as tax_amount,
round((coalesce(t.charged_amount, 0) - coalesce(t.tax_amount, 0)) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as revenue_amount

from Transactions t
left join subscriptions s on t.source_reference = s.globalsubscriptionid
left join plans p on p.priceplanid = s.priceplanid
left join bolt_finint_prod.silver.fi_paymentmethodv2_enriched pm on t.paymentmethodid = pm.id

where 1=1
group by all
)

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
    COALESCE(SUM(t.charged_amount), 0) AS charged_amount,
    COUNT(r.original_transaction_id) AS refunds_count,
    COALESCE(SUM(r.total_refunded), 0) AS total_refunded
FROM transactions t
LEFT JOIN refunds r ON t.event_id = r.original_transaction_id
LEFT JOIN subscriptions s ON t.source_reference = s.globalsubscriptionid
LEFT JOIN plans p ON s.priceplanid = p.priceplanid 
WHERE t.event_type = 'SUCCESSFUL'
    AND t.created_date::date >= '2024-01-01'
GROUP BY ALL
HAVING count > 1
    AND total_refunded = 0;

-- COMMAND ----------

-- DBTITLE 1,Duplicate Transactions V2
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
    -- (t.charged_amount / 100) as Charged_amount,
    COUNT(DISTINCT t.event_id) AS count,
    COALESCE(SUM(t.charged_amount), 0)/100 AS charged_amount,
    COUNT(r.original_transaction_id) AS refunds_count,
    COALESCE(SUM(r.total_refunded), 0) AS total_refunded,
    ARRAY_AGG(DISTINCT t.event_id) AS event_ids,
    ARRAY_AGG(Distinct t.provider_reference_id) as ref_Ids
FROM transactions t
LEFT JOIN refunds r ON t.event_id = r.original_transaction_id
LEFT JOIN subscriptions s ON t.source_reference = s.globalsubscriptionid
LEFT JOIN plans p ON s.priceplanid = p.priceplanid 
WHERE t.event_type = 'SUCCESSFUL'
    AND t.created_date::date >= '2024-01-01'
    and t.userid not in (select userid from test_users)
GROUP BY     
    t.userid,
    t.source_type,
    t.created_date::date,
    t.service_period_startdate::date,
    p.producttype,
    p.productname,
    p.plan_price,
    t.charged_amount
HAVING COUNT(DISTINCT t.event_id) > 1
    AND COALESCE(SUM(r.total_refunded), 0) = 0;

-- COMMAND ----------

select 
t.*,
u.record.user.email as user_email,
u.record.user.testUser as Test_user
from transactions_extract t
left join bolt_dcp_prod.bronze.raw_s2s_user_entities u on u.record.user.userid = t.userid
where upper(t.productname) like '%MAGIC%'
and t.transaction_date >= '2024-07-25T05:00:00.00+00:00'
and userid = 'USERID:bolt:f12fd368-5d5a-4eac-afab-5694139c8dae'
group by all;

-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions
select 
userid,
count(distinct globalsubscriptionid) as subscription_count
from Active_Subscriptions
where provider_type = 'Direct'
and product_type = 'MAIN'
and status in ('ACTIVE','CANCELED')
group by 1
having subscription_count > 1;

-- COMMAND ----------

-- DBTITLE 1,Unbilled Subscriptions
select
  userid,
  globalsubscriptionid,
  subscribedinsite,
  Provider_type,
  provider_name,
  product_type,
  productname,
  paymentperiod,
  campaignname,
  internalname,
  status,
  startdate::date,
  nextrenewaldate::date,
  nextretrydate::date,
  plan_price,
  last_charge::date,
  last_charge_result,
  last_result_reason,
  last_invoice_start_date::date,
  last_invoice_end_date::date,
  last_invoice_amount,
  priceplan_market
from Active_Subscriptions
where 1=1
  and nextrenewaldate < current_date()-18
  -- and nextretrydate is null
  and plan_price > 0
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
from Active_Subscriptions
where 1=1
and paymentperiod in ('MONTH','YEAR')

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
and created_Date >= '2024-01-01T00:00:00.000+00:00'
group by all
Having revenue_amount - plan_price > 0
and total_refunded = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Add-On Controls

-- COMMAND ----------

-- DBTITLE 1,Add-on Live
select *
from active_subscriptions
where 1=1  
and product_type = 'ADD_ON'
and provider_type = 'Direct'

-- COMMAND ----------

-- DBTITLE 1,Add-On Base Sub ID Check
select * from active_subscriptions
where product_type = 'ADD_ON'
and provider_type = 'Direct'
and basesubscriptionID is null

-- COMMAND ----------

-- DBTITLE 1,Active Add-On Terminated Base
with base as 
(
select * from all_subscriptions
where product_type = 'MAIN'
and status = 'TERMINATED'
)
,
add_ons as (
select * from all_subscriptions
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
from all_subscriptions
where product_type = 'MAIN'
and provider_type = 'Partner'
and status in (
  'ACTIVE',
  'CANCELED'
)
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
-- MAGIC # Adhoc

-- COMMAND ----------

-- DBTITLE 1,Auth Rates by Provider
WITH filtered_transactions AS (
  SELECT
    date_trunc('month', transaction_date) AS transaction_month,
    payment_provider,
    payment_type,
    event_type,
    event_id
  FROM transactions_extract
  WHERE event_type IN ('SUCCESSFUL', 'FAILED', 'RETRYING')
    AND transaction_date >= '2024-05-01'
    AND payment_provider is not null
)
SELECT 
  date_format(transaction_month, 'MM-yyyy') as Month,
  payment_provider,
  payment_type,
  SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) AS Successful,
  SUM(CASE WHEN event_type IN ('FAILED', 'RETRYING') THEN 1 ELSE 0 END) AS Failed,
  COUNT(event_id) AS Total_Transactions,
  round(SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) / CAST(COUNT(event_id) as FLOAT) * 100.0, 2) AS Auth_Rate
FROM filtered_transactions
GROUP BY 1,2,3

-- COMMAND ----------

-- DBTITLE 1,Adyen Auth Rates
WITH filtered_transactions AS (
  SELECT
    transaction_date,
    payment_provider,
    payment_type,
    event_type,
    event_id
  FROM transactions_extract
  WHERE event_type IN ('SUCCESSFUL', 'FAILED', 'RETRYING')
    AND transaction_date > '2024-06-24'
    AND payment_provider = 'ADYEN'
)
SELECT 
  transaction_date::date,
  payment_provider,
  payment_type,
  SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) AS Successful,
  SUM(CASE WHEN event_type IN ('FAILED', 'RETRYING') THEN 1 ELSE 0 END) AS Failed,
  COUNT(event_id) AS Total_Transactions,
  round(SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) / CAST(COUNT(event_id) as FLOAT) * 100.0, 2) AS Auth_Rate
FROM filtered_transactions
GROUP BY 1,2,3;


WITH filtered_transactions AS (
  SELECT
    transaction_date,
    payment_provider,
    payment_type,
    event_type,
    event_id
  FROM transactions_extract
  WHERE event_type IN ('SUCCESSFUL', 'FAILED', 'RETRYING')
    AND transaction_date > '2024-06-24'
    AND payment_provider = 'ADYEN'
)
SELECT 
  payment_provider,
  payment_type,
  SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) AS Successful,
  SUM(CASE WHEN event_type IN ('FAILED', 'RETRYING') THEN 1 ELSE 0 END) AS Failed,
  COUNT(event_id) AS Total_Transactions,
  round(SUM(CASE WHEN event_type = 'SUCCESSFUL' THEN 1 ELSE 0 END) / CAST(COUNT(event_id) as FLOAT) * 100.0, 2) AS Auth_Rate
FROM filtered_transactions
GROUP BY 1,2;


-- COMMAND ----------

-- DBTITLE 1,Adyen Auth Rates Detail
  SELECT
    transaction_date::DATE,
    payment_provider,
    payment_type,
    event_type,
    event_id,
    userid,
    globalsubscriptionid
  FROM transactions_extract
  WHERE event_type IN ('SUCCESSFUL', 'FAILED', 'RETRYING')
    AND transaction_date > '2024-06-24'
    AND payment_provider = 'ADYEN'
    group by all


-- COMMAND ----------

-- DBTITLE 1,Magic Test Users - Subs and Trx
select t.*,
tu.email as user_test_email,
tu.testUser as test_user 
from transactions_extract t
join test_users tu on t.userid = tu.userid
where 1=1
and upper(t.productname) like '%MAGIC%'
and event_type IN (
  'SUCCESSFUL',
  'FAILED',
  'RETRYING',
  'REFUNDED',
  'CHARGEBACK'
);

select s.*,
tu.email as user_test_email,
tu.testUser as test_user 
from all_subscriptions s
join test_users tu on s.userid = tu.userid
where 1=1
and upper(s.productname) like '%MAGIC%';

-- COMMAND ----------

-- DBTITLE 0,MAGIC BUNDLE SUBS
select 
s.*,
u.record.user.email as user_email,
u.record.user.testUser as Test_user
from all_subscriptions s
left join bolt_dcp_prod.bronze.raw_s2s_user_entities u on u.record.user.userid = s.userid
where upper(s.productname) like '%MAGIC%'
and s.startdate >= '2024-07-25T05:00:00.00+00:00'
group by all;

-- COMMAND ----------

-- DBTITLE 1,MAGIC BUNDLE TRANSACTIONS
select 
t.*,
u.record.user.email as user_email,
u.record.user.testUser as Test_user
from transactions_extract t
left join bolt_dcp_prod.bronze.raw_s2s_user_entities u on u.record.user.userid = t.userid
where upper(t.productname) like '%MAGIC%'
and t.transaction_date >= '2024-07-25T05:00:00.00+00:00'
group by all;

-- COMMAND ----------

select 
t.*,
u.record.user.email as user_email,
u.record.user.testUser as Test_user
from transactions_extract t
left join bolt_dcp_prod.bronze.raw_s2s_user_entities u on u.record.user.userid = t.userid
where 
-- upper(t.productname) like '%MAGIC%'
userid in ('USERID:bolt:69b72d50-937f-4f85-92d4-97b08847490f',
'USERID:bolt:7a4a533c-4433-4a07-9425-b8f8f5666e7c',
'USERID:bolt:3d6b8afa-9fb4-4aab-9aeb-185c0f22b6c7',
'USERID:bolt:883fe8f0-9c36-460c-b057-b375e1426c7f',
'USERID:bolt:3e21c273-27c1-436e-a34d-37977a4d76db',
'USERID:bolt:334c9e63-a5b5-4fe6-aac9-28786efc0345',
'USERID:bolt:89ef0d10-b581-4475-8ca2-4686270dc57d',
'USERID:bolt:13f88afc-2401-4ae9-af3f-0e26935221ad',
'USERID:bolt:1d381f89-431e-4cdb-8e72-a2ccc3be62af',
'USERID:bolt:47cd2b60-3e15-445f-beb6-d414f163552c',
'USERID:bolt:2de1d252-0b53-401c-b0d8-15699c77df52',
'USERID:bolt:06f4677e-48df-45f6-bc85-0d3d53169e13',
'USERID:bolt:5ad75fb5-4c33-4569-9b57-fe2cb05b78de',
'USERID:bolt:cd232fe6-6e19-44de-a783-6ebcfb07af4e',
'USERID:bolt:1629fe53-6dc1-4ba5-9471-069cad7dd9e9',
'USERID:bolt:5d877eea-4de7-445c-89c9-c316b9ea9f71',
'USERID:bolt:0acfb368-094f-405d-a421-aba5b434d8dd',
'USERID:bolt:10ae7457-97ae-4f86-8a2f-958f71e66f57',
'USERID:bolt:20383e19-26f4-49d7-8371-7048683a7f87',
'USERID:bolt:bd3ce79f-824f-4d90-8439-8187a27286e4',
'USERID:bolt:305a09a1-a5c5-45b0-89a2-79ddf44c63d6',
'USERID:bolt:6802aff8-6f23-495c-aca3-29bcbdcc0182',
'USERID:bolt:17624535-6163-4dba-ae46-a9eabbd71a00',
'USERID:bolt:f9105860-5e84-4bc2-a3ef-d3648410a1f6',
'USERID:bolt:f2e09b2e-fd66-4c53-9e91-08c707d8f70b',
'USERID:bolt:1e0ec847-455c-47f8-9909-327b4fb37003',
'USERID:bolt:c07e454e-553e-49fc-95aa-2301c75f61f6',
'USERID:bolt:ba5f70b0-88a5-43aa-97a0-00bf944c02e1',
'USERID:bolt:3ddc310b-ea4d-484a-8b06-129e8f1e4d6a',
'USERID:bolt:64de057b-7256-47ed-b20b-7bf3f477d7b1',
'USERID:bolt:c4e0c741-b24a-4e47-b996-8f0276ba1c0a',
'USERID:bolt:f3dff3c4-d5dc-457b-98e7-5494828171db',
'USERID:bolt:7c3f8ac5-39a9-4c65-8a15-963f9dad7029',
'USERID:bolt:90c58166-6fde-4252-8524-1ad8c19e7f18',
'USERID:bolt:8bf7ba3e-a556-498e-8fe0-be992e44043a',
'USERID:bolt:6174268f-08fd-410b-909b-38713840d90e',
'USERID:bolt:bbd3e1d5-f494-4517-9e27-2a31e340dae2',
'USERID:bolt:3a86d64c-914b-4e0b-b8e6-2e08b9d30707',
'USERID:bolt:d9c9c307-d0fe-4767-9e95-b5ae6b05a2bf',
'USERID:bolt:1ac9141b-5968-4743-835f-26d11012b313',
'USERID:bolt:461c21fe-5592-4290-b89c-862f59ef5f80',
'USERID:bolt:81ec2e7e-b054-4ef0-9977-88e85d5b8d12',
'USERID:bolt:02c5be72-ac6c-4f33-b6a8-1cebf7dd545d',
'USERID:bolt:ad6871ff-1356-48e1-83ea-60ae64352d52',
'USERID:bolt:6f34927d-fd39-4244-a3b5-846853dbfaaf',
'USERID:bolt:4cc573d9-3b50-43e9-b8f5-d9a6213ba585',
'USERID:bolt:2559d9e7-667c-403b-b717-e1c4c81068f1',
'USERID:bolt:2ee98cbd-8471-4b83-9129-efec1f79def2',
'USERID:bolt:abbc4966-276b-4c27-aff4-4583ac22c34e',
'USERID:bolt:2e73a19a-5c7e-4712-917e-810c299082d1',
'USERID:bolt:2c18203d-b492-4044-848a-285f8adacd90',
'USERID:bolt:3ed46c69-11c9-4d0a-99e9-f56f3ca291c6',
'USERID:bolt:a4addd59-7575-43f9-a1d5-2635964f4bf6',
'USERID:bolt:8328326b-0bbf-4da9-8beb-4968fbbacd83',
'USERID:bolt:6d76ba29-1284-43af-b4d6-71965d974636',
'USERID:bolt:e869ac57-fb5b-4b38-add5-af4d632f8cc3',
'USERID:bolt:6115c2f2-5ba7-4689-882c-d099efb7682b',
'USERID:bolt:e630857b-d571-4439-a31d-5be54fe38c85',
'USERID:bolt:570f7a52-5101-4a4c-8451-a4e18039d49b',
'USERID:bolt:b84760b5-2add-4a75-8a9b-bcceedef168a',
'USERID:bolt:771c4a60-89a1-400c-b8dc-8b88ceb46cb4',
'USERID:bolt:49498f2f-42a0-4016-9c84-4a7902ab1342',
'USERID:bolt:4b02a2ad-6205-401e-a609-1dce75ce4ad8',
'USERID:bolt:687a4bff-e597-4be6-9e0b-2c1b768200f4',
'USERID:bolt:eb99f5a6-3434-48a7-86bc-af5fc0f296b1',
'USERID:bolt:1d3324ba-80bf-4db3-a5df-fa0be273aaec',
'USERID:bolt:c41d71f5-768a-4470-8120-0014a566024c',
'USERID:bolt:712d5fd5-5c3f-4b36-9725-985e86c9a761',
'USERID:bolt:11f4cd70-c8bb-4665-a938-e086ab2edf23',
'USERID:bolt:a83ad04c-68b0-45c7-9b50-1f318142cecc',
'USERID:bolt:5c0b0a06-105a-4a79-beec-dceb436f3ec1',
'USERID:bolt:79cdf891-ff88-4ca2-81ad-bbdd6453b420',
'USERID:bolt:2a4c96ba-74d8-4de2-92d3-d16fc8bb2457',
'USERID:bolt:67b0649f-15ad-4084-95f5-f89ac6374599',
'USERID:bolt:72523ac6-afdd-4bc6-9457-c127e4d5f83d',
'USERID:bolt:94c1d83c-ced4-4e6c-b7fd-9a7390e94123',
'USERID:bolt:d19d3054-d4b4-402f-b60a-2577c752acf5',
'USERID:bolt:c0bc86aa-07da-4cd8-a40c-4e5c4887e848',
'USERID:bolt:9321d708-b5a6-46e8-bb31-4caa78e06771',
'USERID:bolt:9e9d7c18-22ad-4716-956c-0af7e41148a7',
'USERID:bolt:d5b280bf-8e06-4ff0-a9ff-4e92e7825b98',
'USERID:bolt:9c489abf-e582-4879-894a-d7bf51cf11e5',
'USERID:bolt:3b6ea0fd-287c-4de8-a911-56fc888e21e8',
'USERID:bolt:4386d718-5bf2-49f1-93c6-f5f5c84f15d7',
'USERID:bolt:335a771f-d2cc-4f5f-a8d6-9f3a911b853d',
'USERID:bolt:a8c39dd2-84d9-4269-9c33-ec7ae0648290',
'USERID:bolt:d81d929d-62ab-4032-b7eb-14119abce074',
'USERID:bolt:853bf8ef-ea39-4bb3-bdc1-6b3d362931e8',
'USERID:bolt:63a28d55-d26d-4f52-90a8-ea518081cf4a',
'USERID:bolt:1ff9bda6-1b50-4889-9d59-cd2460c171f1',
'USERID:bolt:a468b0e8-aa72-45b9-b549-054b3acd4ded',
'USERID:bolt:5d75bdf1-f2ab-4b6b-a547-fc66c320b113',
'USERID:bolt:54e61123-1d05-4399-b40e-fa08d5e39a47',
'USERID:bolt:986b7156-931c-4c6f-9497-24103a71b8d4',
'USERID:bolt:e4babe54-a083-45c9-a143-aad79dea4461',
'USERID:bolt:2893bb02-008d-45d4-9ffb-599b4b3aab61',
'USERID:bolt:d5fc2a67-9082-4fe2-a4ca-a2ebc146cfa3',
'USERID:bolt:9cc73824-fbef-426f-8c77-8d37a75e6eef',
'USERID:bolt:9f80d045-cd47-4c45-a6be-7a33c2f13128',
'USERID:bolt:1b791c05-b008-4e32-8f7b-688b58212a05',
'USERID:bolt:2143bba7-5424-4093-8ff0-d324cb4d7b77',
'USERID:bolt:7f0f713a-27c0-4ebf-975f-2b646b73d55b',
'USERID:bolt:96af52ae-810e-4cfb-afcf-4cb840363237',
'USERID:bolt:9167287d-4b0f-4483-90ef-1071fee55c9e',
'USERID:bolt:efb7b0c7-29c1-4fa7-bd5c-9d3c936ebc15',
'USERID:bolt:9e607ae3-b014-4c9c-85b5-5621de4bf06f',
'USERID:bolt:32c2310c-24bb-4844-8890-cc1759915026',
'USERID:bolt:ed42dc46-21c0-4dc5-9cb3-8cfc8e8fd0a2',
'USERID:bolt:632afc1e-a481-4950-a08e-d09f88034d89',
'USERID:bolt:5b285b54-0528-4f60-a2d9-a82bba7e9227',
'USERID:bolt:2182e944-9fad-4add-bbde-4fed039ffea3',
'USERID:bolt:5e25941f-8342-41c4-9541-160b981b9cf1',
'USERID:bolt:9cb364b0-7844-44ff-9899-2f9036faa517',
'USERID:bolt:ac332974-5c71-49ef-856d-d300ac921570',
'USERID:bolt:99b8ea4a-9c25-46aa-9a9d-46b14e1a5723',
'USERID:bolt:dc6929e2-b27e-44c5-894f-27ab2718ab18',
'USERID:bolt:fea90973-070e-4918-a1ea-b4cccfdc9f72',
'USERID:bolt:1bbb7fa5-8ea9-4427-a711-36a30aca36d7',
'USERID:bolt:834534fb-452f-4d1a-8a75-66f18691b240',
'USERID:bolt:fabfed7c-a085-4096-9ae6-5ba211ded7aa',
'USERID:bolt:2cf933e2-bfed-4113-8403-801b7dbbdc2b',
'USERID:bolt:0a73f4a4-c626-4dde-86dd-2ac8b027a7ef',
'USERID:bolt:6a6592aa-ba89-4b2a-8847-7fa4948886b9',
'USERID:bolt:fef678dc-3652-4d75-8886-f7ab71a2a68c',
'USERID:bolt:fabd3ace-2c5a-403f-a736-2cd208e11ea4',
'USERID:bolt:6c5b408a-7cbf-4490-a77e-0c82deb67f96',
'USERID:bolt:48d018d8-bdb7-4b6d-9241-effff40f4156',
'USERID:bolt:5398ccac-4316-4bb0-b311-566a88dd376c',
'USERID:bolt:8dbc88eb-d533-4ba8-9335-942f22775e12',
'USERID:bolt:20845fe5-8932-47f5-9943-a7f99ad4eb0e',
'USERID:bolt:d01f2a23-c322-4941-bf18-d5d4d939c8d1',
'USERID:bolt:8a395f59-0e48-4e7d-a32a-460d08d3468c',
'USERID:bolt:b968324c-ff4c-43fc-b568-a12f5a822e4e',
'USERID:bolt:d0ba9f65-7998-424b-9430-238b3e13e063',
'USERID:bolt:52f48620-4677-4a2f-a73c-2d57f3213ff2',
'USERID:bolt:bc2cf829-6521-4b2b-a769-c31c27960134',
'USERID:bolt:a66271f2-38c4-4d78-86af-7a9452ef8ac9',
'USERID:bolt:8ff65f53-4b10-4be4-b64d-51a0672df54b',
'USERID:bolt:ca2694e6-0270-4df9-8fa1-e60f8807ed06',
'USERID:bolt:eede2107-47a1-4feb-9f56-efe1adb9164c',
'USERID:bolt:824fa837-8ec7-4c1a-a131-25ce916e9fc9',
'USERID:bolt:d1129ed5-8bff-4a7f-9306-97f42a11f74f',
'USERID:bolt:938d566d-3b2b-4ef6-9e76-56cfdab3b5e2',
'USERID:bolt:abfd7732-41de-4f13-9a65-7e275d92673b',
'USERID:bolt:cb74cd59-5694-4012-ae5b-5550d9ec4454',
'USERID:bolt:78d8babd-30c1-44be-a74e-aad3b71c30dc',
'USERID:bolt:57f2d721-e32f-438b-9d6c-f04faf319266',
'USERID:bolt:67aa42f1-958a-4457-b775-a1e04e6ac362',
'USERID:bolt:e4a2e596-790e-498c-b1ee-b3434f9c2cff',
'USERID:bolt:16404d2f-89b0-4216-8b4d-33288be847d0',
'USERID:bolt:77e4c239-0e33-46c8-812d-fdd5d8cc384f',
'USERID:bolt:9fb0224d-dbd2-4d2c-b3d1-5e1786fa1eab',
'USERID:bolt:670da44b-3e65-4566-bc29-849f67e7838f',
'USERID:bolt:80b08dda-9c55-4f8e-a3fb-5711dcf02b1a',
'USERID:bolt:7539828a-4b12-4d28-a0c8-e189a81ebf61',
'USERID:bolt:70ad05be-4cf7-490a-993f-d748414ebd7f',
'USERID:bolt:9f5f6c12-21e7-4d44-a984-06d9fbc50c11',
'USERID:bolt:c8b78f39-5dbd-4a85-a90a-f2b1ba633034',
'USERID:bolt:93471dc5-b19b-4924-9ece-4d8e7f5320b7',
'USERID:bolt:8e5aeaf2-82ec-4724-8fcc-a611beeb226b',
'USERID:bolt:ad12a3a6-3699-4e80-a0cb-aa7f406bcb09',
'USERID:bolt:ed85338a-13d3-4390-a670-a73db40ebb59',
'USERID:bolt:c7ce5ba8-6b60-4e88-b0d2-cf9df6cf2ab1',
'USERID:bolt:3e393cab-98b4-46a0-b383-5e9a39bfd0cb',
'USERID:bolt:9ad4a5ac-4706-46e7-a6af-a189bbe4181e',
'USERID:bolt:d32602cf-ff62-48e0-8b74-0de94e35ea5a',
'USERID:bolt:d233d810-5d89-4da6-a809-ae5ac1404099',
'USERID:bolt:83905667-7881-4afc-adee-5ab1853013cf',
'USERID:bolt:13f0fb21-146e-4c02-87bc-a6da5a4472f2',
'USERID:bolt:6cc79cc4-f729-4d38-abad-e9983ba6eb35',
'USERID:bolt:3f6bfb8f-9cca-4455-8d06-2776686661d7',
'USERID:bolt:bcf74842-3e0a-4264-a25b-ae2f1f093c6d',
'USERID:bolt:f2dabbc9-a587-4ef8-9412-b81955da6cb4',
'USERID:bolt:dc0b20c4-c970-4d09-bbc2-ffa62b122c0e',
'USERID:bolt:38ab80c8-57eb-43ec-86be-71560c2f1294',
'USERID:bolt:a3172129-8ac3-4a8c-a6f0-e0ad09e21cba',
'USERID:bolt:a8f90f7c-dcfa-4665-8abe-89a66d8c7b24',
'USERID:bolt:13cd8d30-e9bd-4483-a553-acdf124fbed0',
'USERID:bolt:59d81144-04f2-46ab-b22b-6407526a4117',
'USERID:bolt:f0e64b8f-56d6-44f2-ba32-bd8ed2f887eb',
'USERID:bolt:82ac82e3-5431-4863-999f-0a6dbf27f72b',
'USERID:bolt:d292816d-31a8-4731-8fee-38102e6490aa',
'USERID:bolt:617645d1-11ff-4202-b37f-01266ed48be0',
'USERID:bolt:1bc81156-0502-47d7-8219-ebcad63f18ae',
'USERID:bolt:b14db861-57f8-42d5-9bcf-639413855733',
'USERID:bolt:8c84358a-7c70-4084-9f26-766a58c78007',
'USERID:bolt:4371d54c-2f91-417a-8ce9-d43c50e51d64',
'USERID:bolt:6e0b2cac-c39b-49ce-b240-9a2e212c7a7d',
'USERID:bolt:c92256a0-88a1-443f-aa75-5e7bf40c42e1',
'USERID:bolt:0972ec28-e2c3-4f6f-a7e4-7b6d577f9b22',
'USERID:bolt:12661262-c16c-48aa-8fb7-6e72265f06ce',
'USERID:bolt:c95ac627-55b2-4bf5-8153-482f739e10c3',
'USERID:bolt:09e374d4-b972-4e9e-8de6-1b643659c8f4',
'USERID:bolt:332b02db-1fa4-4fc6-abdf-99667e829117',
'USERID:bolt:48e8b948-6b3d-4b9d-abff-0d18e5d1bb46',
'USERID:bolt:fe247845-7eb3-4772-aefa-6feae38a7315',
'USERID:bolt:cc093034-423d-4d0b-afc8-5b1bbeef6e4e',
'USERID:bolt:27a621a4-b196-4024-a264-e5171ba6e7bb',
'USERID:bolt:4686a220-b6a6-493f-afc5-5b99273601d0',
'USERID:bolt:e1b07558-37d7-4625-90cb-848eb43f3cc5',
'USERID:bolt:eae4f5fa-be78-48dc-bdb1-b5509ce43b66',
'USERID:bolt:43f4f76a-0959-4a50-bcbc-b8ed9acf4ee7',
'USERID:bolt:1c8b1b6f-400c-47b4-ad09-0980767885d9',
'USERID:bolt:55d7edac-ae52-416d-ba6c-a9a9117eee64',
'USERID:bolt:24632baf-8393-44b2-a035-15d974d79f32',
'USERID:bolt:5b44fd6b-fa78-47d7-b3cb-ea709b8434b5',
'USERID:bolt:80e229b4-2771-498a-9b0f-a733383517e5',
'USERID:bolt:6aba4185-1818-4309-902b-f2b4635589bb',
'USERID:bolt:5e0a0eb2-a7ea-421f-b6bf-b78786d376f6',
'USERID:bolt:f94b1fce-d238-4354-90b7-fe7b2731b44c',
'USERID:bolt:bd873a22-f343-4757-ae55-d9637942b1a3',
'USERID:bolt:7afcc7f9-3ef4-4d6e-80ba-0de18bf4a05e',
'USERID:bolt:7df4da11-75f5-4463-a536-1e7662fc9a16',
'USERID:bolt:2377c7d7-4fbd-4a95-a052-86f90e0a4bbb',
'USERID:bolt:99a4fe28-f398-4da4-b9e3-41dd75b7e707',
'USERID:bolt:f39642fa-ac0b-468b-9227-d9663b55044e',
'USERID:bolt:848bc88d-8189-4556-9dd3-e8e60dd47574',
'USERID:bolt:9674830d-73b4-4b12-8b7f-986dc400c323',
'USERID:bolt:e9b45802-0ca8-47d3-8ca3-19e47551fdc2',
'USERID:bolt:a25a3d98-358e-403e-832d-cb990d4bb3a8',
'USERID:bolt:fb7f48d6-95c5-404f-81e4-a340707c3bbd',
'USERID:bolt:fcbc388f-a9ec-4b12-a45c-45ae1c24f033',
'USERID:bolt:00db8b65-e630-47bb-88c4-6022d3376a0c',
'USERID:bolt:1bdf061c-0967-48e3-9cf5-c9bed0c9e740',
'USERID:bolt:68e104e4-3989-4c68-be87-5f6843a3780c',
'USERID:bolt:7ff6927d-ff0f-4d2c-a4a4-af21c3d5d7ad',
'USERID:bolt:e2c10077-3b22-4350-a36f-77ba6a149b6c',
'USERID:bolt:0f497d77-0894-4c61-b038-4c099d3d25f6',
'USERID:bolt:6c13d21a-8912-4a24-84fc-42489b538c36',
'USERID:bolt:68b143a8-0259-4e36-8aad-1b51f67c6f70',
'USERID:bolt:6f801bb6-fc34-4029-a8e4-ff6702324943',
'USERID:bolt:7d21a8d3-6236-4581-aadc-72dcbfc54b55',
'USERID:bolt:a258af74-ff15-4881-8f5f-818540f9c38e',
'USERID:bolt:893a6950-28c8-4373-b30a-482f86fe701b',
'USERID:bolt:3083064f-f089-46b9-a5d8-93aff888eebb',
'USERID:bolt:21c0f47c-5749-4dc7-9adf-9f8f693c089f',
'USERID:bolt:bbd5be77-ef5e-4594-8837-2ab9d2423856',
'USERID:bolt:9e29387b-5987-4ac8-a24a-a8644e7f5e12',
'USERID:bolt:62bddb40-42c1-43ca-b7b7-8a2c88251f43',
'USERID:bolt:d7b5cb8c-d240-4f59-9010-8c0bae869cba',
'USERID:bolt:3dcfd3a6-1337-4b8b-9a91-89ad1a6b4704',
'USERID:bolt:2eddbf2a-3c2a-4dca-8a35-ed5c753d3aca',
'USERID:bolt:fd25798b-166b-4cae-be94-7ee9e184fdb8',
'USERID:bolt:4ebed299-e685-45cf-9224-ebd5b8226606',
'USERID:bolt:95a9feb1-6748-4742-acb1-b9e6af40a720',
'USERID:bolt:6c7ae68d-5646-4d2d-b0ba-f1549c61bdcb',
'USERID:bolt:4f9a6a35-e87b-4810-b4e1-caec2e507d14',
'USERID:bolt:a3982c0d-8184-4204-87ad-3ea0c095ca20',
'USERID:bolt:3bd4788c-c474-4075-9bb5-a76eab689e51',
'USERID:bolt:22de0a53-9cc4-45d7-a27a-d2cbf6232795',
'USERID:bolt:4b649877-b70a-4db3-8ddd-af5577d9dce0',
'USERID:bolt:6dae424c-5f2c-4d69-9c2b-f23e49de88da',
'USERID:bolt:2cac8dce-aba2-472b-8b43-c5c50c8c009a',
'USERID:bolt:afc2c0c2-dedc-453e-acfd-f78d88f768b7',
'USERID:bolt:d3a904be-1e2d-46f6-8807-b44905cd0b0c',
'USERID:bolt:5311060c-b561-4e0c-b09f-d3c0ce5a2ec3',
'USERID:bolt:200a784c-762c-4ab2-bf62-bf7c6656f88c',
'USERID:bolt:44544535-d4a0-42bd-9215-5a15e60d553d',
'USERID:bolt:858079fa-0d28-48ec-8cd5-d3fb9bef149f',
'USERID:bolt:4f26a813-c506-42de-936c-4f1f38e4e2bb',
'USERID:bolt:b3b8b6e6-b3d2-420b-ac8a-0235909e58fe',
'USERID:bolt:9fe9b494-9e04-4e48-8279-c58d8e717951',
'USERID:bolt:a655dd38-5917-4763-bfbb-f40fbfcb771c',
'USERID:bolt:b2f3f6b4-d975-4950-955c-580b734f08bf',
'USERID:bolt:d924736e-1194-4cd0-83aa-dd9e83bdb1a5',
'USERID:bolt:472ea5c6-2038-4644-a02d-cf622d01dafa',
'USERID:bolt:8b797a44-b46c-45f8-8c8c-f93742b36359',
'USERID:bolt:c6c53da3-a59b-4e13-b20b-3d03484d6104',
'USERID:bolt:4a07f7dd-2053-4ff1-a214-4010dd758f84',
'USERID:bolt:1d0374be-95fa-444a-97eb-93ffa64cf9c1',
'USERID:bolt:11e7f2f3-ea4c-445b-b315-deb6a0fd6e48',
'USERID:bolt:8dbf58f1-9162-439d-af98-d6e2c8541a71',
'USERID:bolt:be42540a-ad46-43a0-9d43-7e4cd01a1fb8',
'USERID:bolt:a069cf00-1481-439c-8d72-c09bbb2d624d',
'USERID:bolt:1c30349d-3b9c-4acd-b203-6f8fcc7b8daf',
'USERID:bolt:2b008299-bb8b-44ca-88a8-f7a0ba52f10e',
'USERID:bolt:56bf1032-5d17-4c00-9486-25d299acc133',
'USERID:bolt:fef0a90a-9bf7-488d-969a-c4b57e4c42f2',
'USERID:bolt:9f297787-fea2-4f11-b7ca-890c6473910b',
'USERID:bolt:af9ffd43-91b2-4b51-a649-28f5baf7be7a',
'USERID:bolt:4ef7cdbd-dc5d-44b7-9d17-dd5d22f779c0',
'USERID:bolt:64e857bb-2f5b-4422-b7f6-c62144fec127',
'USERID:bolt:767b48a6-9853-4536-a8cb-6d21063a58ca',
'USERID:bolt:f7c7863d-f7eb-402b-a6bc-81fe112d55dd',
'USERID:bolt:a84e1988-7208-4e09-87fb-fd8fdfd140b8',
'USERID:bolt:7a157d3b-15cf-4c3f-a56c-6e80e8564bff',
'USERID:bolt:1e1e562f-dc54-43d2-bd5a-5fd877477252',
'USERID:bolt:ab491d91-6b8c-4cf1-a2aa-2dbd96b870c0',
'USERID:bolt:53ad8a8d-a578-4155-80af-e10c4f0dbcdd',
'USERID:bolt:d3077bf6-dd0c-4ace-9faa-cd0ac8827671',
'USERID:bolt:6b7e755c-2066-46cb-ab99-d2e4f3e585e7',
'USERID:bolt:1d238c99-1957-426e-8d3b-e928c07e2904',
'USERID:bolt:47dfb105-bfbe-433b-bd74-2d843ecfba96',
'USERID:bolt:4cdea269-a9ef-4cc4-849c-5384662e772c',
'USERID:bolt:bd736608-2837-49e4-8314-3da1f23dc47e',
'USERID:bolt:6e322547-2629-4607-9e01-23b5e8530d52',
'USERID:bolt:8c85e4ce-6762-4e48-b4a4-b33dfa0d3a28',
'USERID:bolt:8ea2f098-433e-4a4f-97a8-064c036e2b7c',
'USERID:bolt:6b52d367-458e-4fc2-9bfb-463696d53f47',
'USERID:bolt:d0a20799-5c1e-4bd8-a8f8-c35b68a634e9',
'USERID:bolt:357af386-da47-4953-9760-108088dd3c8d',
'USERID:bolt:d5232187-0b01-49b8-8080-727eb1119a1f',
'USERID:bolt:f223fedb-c14f-4f04-85b1-c18e2037bc7a')
-- and t.transaction_date >= '2024-07-24T00:00:00.00+00:00'
and t.event_type IN ('SUCCESSFUL','REFUNDED')
group by all;

-- COMMAND ----------

select * from test_users
where email IN (
'alexandr.gavrilita-02-07-2024-01@wbd.com',
'alexandr.gavrilita-03-07-2024-01@wbd.com',
'alexandr.gavrilita-07-08-2024-20@wbd.com',
'hector.hernaez2@gmail.com'
'hector.hernaez3@gmail.com',
'purchase-swithc-activation-test@wbd.com',
'eligibility-check-magic-existing-max@wbd.com',
'alex+disney1@test.com',
'philibeamuat+8JLQB3AMmagic0001@gmail.com',
'philibeamuat+8JLQB3AMmagic0002@gmail.com',
'philibeamuat+8JLQB3AMmagic0003@gmail.com',
'philibeamuat+8JLQB3AMmagic0004@gmail.com',
'philibeamuat+8JLQB3AMmagic0005@gmail.com',
'hesham.aly+prd+magic1@wbd.com',
'hesham.aly+prd+magic2@wbd.com',
'heshamaly123+prd+magic1@gmail.com',
'heshamaly123+prd+magic2@gmail.com',
'heshamaly123+prd+magic3@gmail.com',
'TestOnMax@gmail.com',
'bundletesting@gmail.com'
'TestingTheBundle@gmail.com',
'maxtestaccount@gmail.com',
'max_testing_utestfordisney+125@disneyplustesting.com',
'alexandr.gavrilita+regression-test-23-07-2024@wbd.com',
'alexandr.gavrilita+regression-test-24-07-2024-1@wbd.com',
'disney-max-bundle-test-24.07.2024@wbd.com',
'algavmd+12345@gmail.com'
'algavmd+utestfordisney@gmail.com',
'MaxandDisneyTest@gmail.com',
'MaxandDisney1@gmail.com'
'joyabillings+disneywads@gmail.com',
'livetest+h9zka4d71m@wbd.com',
'tim.sassanian+1235678@wbd.com',
'timothy.sassanian+1234@gmail.com',
'timothy.sassanian+h9zka4d71m@gmail.com'
)
and testuser is null
