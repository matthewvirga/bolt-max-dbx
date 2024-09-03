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

-- DBTITLE 1,Transactions V2
create or replace temporary view Transactions_v2 as
(

select

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.refundReference
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.providerChargebackReference
  else id
end as event_id,


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


--AMOUNT DETAILS STRUCT
amountDetails.currencyCode as amountDetails_currencyCode,
--simple amount
amountDetails.simpleAmount.minorAmount as amountDetails_simpleAmount_minorAmount,
amountDetails.simpleAmount.taxationCountryCode as amountDetails_simpleAmount_taxationCountryCode,
--amount with tax
amountDetails.amountwithTax.minorAmountInclTax as amountwithTax_minorAmountInclTax,
amountDetails.amountwithTax.taxMinorAmount as amountwithTax_taxMinorAmount,
amountDetails.amountWithTax.taxRateMinorUnits/10000 as amountDetails_amountWithTax_taxRate,
amountdetails.amountWithTax.components[0].description as tax_description,
amountDetails.amountwithTax.taxRateMinorUnits as amountwithTax_taxRateMinorUnits,
amountDetails.amountwithTax.taxationCountryCode as amountwithTax_taxationCountryCode,
amountDetails.amountwithTax.taxDocumentReference as amountwithTax_taxDocumentReference,

--STATE TRANSITIONS STRUCTS
--simple amount
st.event.refunding.amountDetails.simpleAmount.minorAmount as refunding_amountDetails_simpleamount_minorAmount,
st.event.refunding.amountDetails.simpleAmount.taxationCountryCode as refunding_amountDetails_taxationCountryCode,
st.event.refunded.amountDetails.simpleAmount.minorAmount as refunded_amountDetails_minorAmount,
st.event.refunded.amountDetails.simpleAmount.taxationCountryCode as refunded_amountDetails_taxationCountryCode,
st.event.chargeback.amountDetails.simpleAmount.minorAmount as chargeback_amountDetails_minorAmount,
st.event.chargeback.amountDetails.simpleAmount.taxationCountryCode as chargeback_amountDetails_taxationCountryCode,
--amount details
st.event.refunding.amountDetails.amountWithTax.minorAmountInclTax as refunding_amountWithTax_minorAmountInclTax,
st.event.refunding.amountDetails.amountWithTax.taxMinorAmount as refunding_amountWithTax_taxMinorAmount,
st.event.refunded.amountDetails.amountWithTax.minorAmountInclTax as refunded_amountWithTax_minorAmountInclTax,
st.event.refunded.amountDetails.amountWithTax.taxMinorAmount as refunded_amountWithTax_taxMinorAmount,
st.event.chargeback.amountDetails.amountWithTax.minorAmountInclTax as chargeback_amountWithTax_minorAmountInclTax,
st.event.chargeback.amountDetails.amountWithTax.taxMinorAmount as chargeback_amountWithTax_taxMinorAmount,


-- Determines the type of payment event
-- CREATED default means the payment StateTransitions was empty, likely meaning the transaction was created and nothing else yet (first step).
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
  else 'CREATED'
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

from bolt_finint_prod.beam_emea_silver.fi_transactionv2_enriched
LATERAL VIEW explode_outer(stateTransitions) st as event -- this explodes out every state in the StateTranstions struct - should mirror events
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
  (coalesce(t.charged_amount, 0)) /100 as charged_amount
  from transactions t
  left join bolt_finint_prod.beam_emea_silver.fi_paymentmethodv2_enriched pm on t.paymentMethodId = pm.id
  where event_type IN ('SUCCESSFUL','UNKNOWN','PENDING','FAILED', 'RETRYING')
  and t.charged_amount != 0
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
group by all

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
pm.type as payment_method_type,
pm.cardDetails.cardProvider as card_Provider,
pm.cardDetails.fundingSource as funding_source,
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
s.previoussubscriptionglobalid,
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

-- DBTITLE 1,Successful Volume Check (New + Recurring)
select
    transaction_date::date as date,
    event_type,
    launch_wave,
    count(distinct event_id) as quantity
from transactions_extract
where 
    (event_type = 'SUCCESSFUL' and created_date >= '2024-06-10T23:00:00.000+00:00')
    or 
    (event_type = 'REFUNDED' and transaction_date >= '2024-06-20T23:00:00.000+00:00')
group by 
    transaction_date::date, 
    event_type, 
    launch_wave

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
-- and provider_type = 'Direct'
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
    -- AND t.created_date >= '2024-05-20T23:00:00.000+00:00'
  and ( (p.launch_wave IS NULL AND t.created_date::date >= '2024-05-20T23:00:00.000+00:00') OR (p.launch_wave IS NOT NULL AND t.created_date >= '2024-06-11T00:00:00.000+00:00') )
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
    ARRAY_AGG(DISTINCT t.event_id) AS event_ids,
    ARRAY_AGG(Distinct t.provider_reference_id) as ref_Ids,
    t.created_date::date,
    t.service_period_startdate::date,
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
and ( (p.launch_wave IS NULL AND t.created_date::date >= '2024-05-20T23:00:00.000+00:00') OR (p.launch_wave IS NOT NULL AND t.created_date >= '2024-06-11T00:00:00.000+00:00') )
GROUP BY t.userid, t.created_date::date, t.service_period_startdate::date, p.productname, p.plan_price
HAVING COUNT(DISTINCT t.event_id) > 1
    AND COALESCE(SUM(r.total_refunded), 0) = 0;

-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions
select
userid,
count(distinct globalsubscriptionid) as subscription_count
from active_subscriptions
where provider_type = 'Direct'
and status in ('ACTIVE','CANCELED')
and product_type = 'MAIN'
-- and launch_wave is null
group by 1
having subscription_count > 1

-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions DETAIL
with dupes as 
(
select
userid,
count(distinct globalsubscriptionid) as subscription_count
from active_subscriptions
where provider_type = 'Direct'
and status in ('ACTIVE','CANCELED')
and product_type = 'MAIN'
-- and launch_wave is null
group by 1
having subscription_count > 1
)

select s.* from active_subscriptions s
JOIN dupes on s.userid = dupes.userid
where s.status in ('ACTIVE','CANCELED')

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
  and launch_wave is not null
  and nextrenewaldate <= current_date()-16
  -- and nextretrydate is null
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
  and ( (t.launch_wave IS NULL AND t.created_date::date >= '2024-05-20T23:00:00.000+00:00') OR (t.launch_wave IS NOT NULL AND t.created_date >= '2024-06-11T00:00:00.000+00:00') )
group by all
Having charged_amount - plan_price > 0
and total_refunded = 0


-- COMMAND ----------

-- DBTITLE 1,SEPA Direct Debit Check
WITH transaction_events AS (
  SELECT
    event_id,
    priceplan_market,
    payment_method_type,
    funding_source,
    provider_reference_id,
    event_type,
    transaction_date,
    LEAD(transaction_date) OVER (
      PARTITION BY provider_reference_id 
      ORDER BY transaction_date
    ) AS next_transaction_date,
    LEAD(event_type) OVER (
      PARTITION BY provider_reference_id 
      ORDER BY transaction_date
    ) AS next_event_type
  FROM transactions_extract
  WHERE event_type IN ('SUCCESSFUL', 'PENDING')
    and funding_source = 'IDEAL_RECURRING'
),

pending_to_success AS (
  SELECT
    event_id,
    priceplan_market,
    payment_method_type,
    funding_source,
    provider_reference_id,
    event_type,
    transaction_date,
    next_transaction_date,
    next_event_type,
    CASE
      WHEN event_type = 'PENDING' AND next_event_type = 'SUCCESSFUL' THEN
      TIMESTAMPDIFF(HOUR, transaction_date, next_transaction_date)
      ELSE NULL
    END AS time_diff_hours
  FROM transaction_events
)

SELECT
  event_type,
  priceplan_market,
  payment_method_type,
  funding_source,
  transaction_date::date,
  next_transaction_date::date,
  next_event_type,
  time_diff_hours,
  provider_reference_id
FROM pending_to_success
WHERE time_diff_hours = 0
group by all


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

-- DBTITLE 1,Test Subscriptions
select
s.userid,
s.launch_wave,
u.record.user.email,
s.product_type,
s.productname,
s.priceplan_market,
s.globalsubscriptionid,
s.status
from active_subscriptions s 
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on s.userid = u.record.user.userid
where s.status = 'ACTIVE'
  and UPPER(s.productname) not like '%LEGACY%'
  and lower(u.record.user.email) like '%8jlqb3am%'
  and s.provider_type = 'Direct'


UNION

select
s.userid,
s.launch_wave,
u.record.user.email,
s.product_type,
s.productname,
s.priceplan_market,
s.globalsubscriptionid,
s.status
from active_subscriptions s 
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on s.userid = u.record.user.userid
where s.status = 'ACTIVE'
  and UPPER(s.productname) not like '%LEGACY%'
  and ( (launch_wave IS NULL AND s.startdate <= '2024-05-20') OR (launch_wave IS NOT NULL AND s.startdate <= '2024-06-10') )
  -- and lower(u.record.user.email) like '%8jlqb3am%'
  and s.provider_type = 'Direct'


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
-- t.*,
t.userid,
lower(u.record.user.email) as email,
t.provider_reference_id,
t.merchantaccount,
t.currency,
t.payment_method_type,
t.funding_Source,
t.charged_amount,
coalesce(r.total_refunded,0) as total_refunded,
round((t.charged_amount + coalesce(r.total_refunded,0))*-1,2) as refund_amount
from transactions_extract t
left join subscriptions s on t.globalsubscriptionid = s.globalsubscriptionid
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on t.userid = u.record.user.userid
left join refunds r on t.event_id = r.original_transaction_id
  where event_type = 'SUCCESSFUL'
  and UPPER(productname) not like '%LEGACY%'
  and lower(u.record.user.email) like '%8jlqb3am%'
group by all
Having (charged_amount + total_refunded) > 0
and refund_amount < 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ad-hoc

-- COMMAND ----------

-- DBTITLE 1,Wave 2 BRIM Totals
select
    transaction_date::date as date,
    event_type,
    launch_wave,
    Priceplan_market,
    product_type,
    count(distinct event_id) as quantity
from transactions_extract
where 
    ((event_type = 'SUCCESSFUL' and created_date >= '2024-06-11T00:00:00.000+00:00')
    or 
    (event_type = 'REFUNDED' and transaction_date >= '2024-06-11T00:00:00.000+00:00'))
    AND launch_wave = 'WAVE 2'
group by 
all

-- COMMAND ----------

-- DBTITLE 1,test refunds check
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
-- t.userid,
-- lower(u.record.user.email) as email,
-- t.provider_reference_id,
-- t.merchantaccount,
-- t.currency,
-- t.fundingSource,
-- t.charged_amount,
coalesce(r.total_refunded,0) as total_refunded,
round((t.charged_amount + coalesce(r.total_refunded,0))*-1,2) as refund_amount
from transactions_extract t
left join subscriptions s on t.globalsubscriptionid = s.globalsubscriptionid
left join bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities u  
  on t.userid = u.record.user.userid
left join refunds r on t.event_id = r.original_transaction_id
  where 
  t.event_type = 'SUCCESSFUL'
  and t.provider_reference_id IN (
'F7KXQDDRV8H5ZLF6',
'S5JXP4QSRJ435LF6',
'VQPF74WP22MP9RF6',
'FD4C4TTR57F6H953',
'B4Q62VBP93NP3RF6',
'VXNJX9N777ZXG6X3',
'LXH36G6F5S9M6553',
'V9KJ9WTBSDMRGHG2',
'N2SSWH3NRJ435LF6',
'RL3WH9Z695W3CLF6',
'NRZ4LDKKFWJPV832',
'HRZWJ3KLX5RNTM52',
'ZMXN27XSX6DJGGF6',
'SS35ZQLKLC228GX3',
'CBP82KJ3HLVFW9X3',
'LZQCFL6XQMGQ9RF6',
'DMFKJJHN93NP3RF6',
'X3J9S9NT3CR59953',
'DVTPHBP65TZMTQF6',
'K5NLK48RDQBR8653',
'XMXHWPNHRJ435LF6',
'F6NBNJJ32X24J4C2',
'HCHQ6PBCG3ZRC2C2',
'WKNKVR6MV6WH7XB2',
'L925W4PNFMBWJ753',
'T4J94XRSX6VW8KF6',
'KKJ8ZKDK66P5Q4C2',
'QS28P4SCF67XRP52',
'PKMHDRBW73NVM232',
'RT66PZFW9F3DHK52',
'C8LBPB4PQZTDPK52',
'LX63BJWJWMKH5TD3',
'KFMBGTJ32X24J4C2',
'WFBDQBR3WNTQ4932',
'DFCNXDJF3TZSF232',
'SJZDT3VNBTNGFC53',
'Q3PSBRWDHJH99VB2',
'GBCNQJ7R3H577MF6',
'TBXRJKW6F6GL6553',
'CM6835PZZGXFPSD3',
'QT9682CFXKMKX453',
'LZBCCFXBMKC5W3F6',
'PHCK48CFXKMKX453',
'TR5V6462QLWTPWD3',
'GF9B6XDZGK5XG6X3',
'QLJKHM6MV6WH7XB2',
'X7QSHXZRBS5BFF32',
'Z7ZZ2W6W65QBTRD3',
'GX9LSF7SGKW29C32',
'MGQ52RMJKZNT5SF6',
'HLG2MLBNLBT95B53',
'DDWS3FKWD67MPPG2',
'PDNQX962LXKJ8GF6',
'VBQZ836PD67XRP52',
'J7HHGS286BJ5RLF6',
'SGFWSQC659L678X3',
'NWPK2B6CHKD9Q2F3',
'DLZCXJGL92W7MMF6',
'FQLD5VGBMDCGFC53',
'WN3RXN7GQMGQ9RF6',
'KK3TG9ZRBS5BFF32',
'QRGTZ88LFRNXGKF6',
'SZWLRRQR2KDKGGF6',
'HNWJGNFSTCTS5SF6',
'PM4TQQMJJHJQQ4X3',
'KW32FXDXCRCPXPB2',
'RTJJRFRR2HXP3DX3',
'ZCQMWQXV9F3DHK52',
'RXN7W6XP22MP9RF6',
'K8HJBS8TRHCKNGF6',
'R9G4485RDQBR8653',
'JZVFWFKW3FC8ZQD3',
'GJSK3GGM7NDLFQF6',
'KX9WT6DTCQBR8653',
'MBPND9QCF67XRP52',
'FQF6ZXXZKD63LWW3',
'KD4TZLVJ8F9DPNF6',
'PBVRDW5TGJT4Q4C2',
'LC8CH823DRTBSJX3',
'C5Z9PSSHLC228GX3',
'CVH69HZKBXMTCSF6',
'RGBMX2JSVDQ3CLF6',
'V6GGDNJX49RMBHF6',
'1RP95814840118436',
'0KV868734N737572T',
'J58JSNQ4H5KF5PF6',
'BD8RL7XBLSQJ7XB2',
'3CN95150SR995111L',
'N8FLTJNC42JTF232',
'P8JGCPC3F2S67MF6',
'FWNQ6G6F5S9M6553',
'NDLHJ74RQGTTF232',
'3CA09218KW549120X',
'TJ8VGJ494KJ8W953',
'NWWD24BVJJB5ZLF6',
'J4F59R92WDQ3CLF6',
'MCGQ32GZZX423C32',
'ZN8LSNV5G5GBKB53',
'K6LNJ6LRHG2LKTD3',
'D3WL5KV8BDVJ83X3',
'XBSGFH4VZDZTCSF6',
'DQ6T3QW833JXGKF6',
'NQTRN7W5RX2FW9X3',
'GF2MH35KMC7HCPF6',
'2EL5021662304734P',
'DSNPCXXXX39DRB53',
'Q7D95B8LGPDVS5X3',
'D6TFBRPXJH33CLF6',
'KFRPM2GRQX4RG653',
'FJ6RSLRNMPJC3SD3',
'RT7H47FGKFGTLJF6',
'GZDM5NMT8MTZ7GX3',
'LBSRG6VVKNK3CLF6',
'SJ2P7GRJHGQH7XB2',
'0PX51872J2702245U',
'WMHMQ9ZW2KDKGGF6',
'VBHKQJP9TQZXZP52',
'Q5LZQBVHCCJ5SGD3',
'3R4212325Y767273F',
'V2QL3SGRJHSFQ632',
'F32QGWNJF6TR8HG2',
'G7XKNJRGS9CNTQF6',
'GLQ7NF9ZJ7CNWPG2',
'ZQQL67VGRR6KG3X3',
'V5G32JVGRR6KG3X3',
'JK3RG6B7VK3CTRD3',
'V94JH4PFFZ8BMRD3',
'L4NMP84G7NDLFQF6',
'DFVFG3RMVHCQ4932',
'NVWPSVXFLFLNTM52',
'WPT994VGRR6KG3X3',
'K64T94WTBN6GFC53',
'CZ9G2BXTBN6GFC53',
'KZGLCGFQKNGXQ753',
'RG27WRQC2DVJGGF6',
'RBCBS93FS3699VB2',
'6PK20088G3909742U',
'ZK8XDZZHJHJQQ4X3',
'N9FPTCJBKNN97F32',
'C7JX325VZDZTCSF6',
'R5L8BJH3M6PGCPF6',
'PKWXQ4RC2DVJGGF6',
'BKP6X3MV78FJ8GF6',
'NZMTHRCLZRLZ2C32',
'V3SSF5RS3742W6X3',
'PK2S8L5J8CHFHSD3',
'HJ9G9494W6KNQPB2',
'PF53MQ5MV6WH7XB2',
'BP58TTJ6LKRSHWD3',
'PGJJSXVFLDPC6FF6',
'7XM56808UM2616620',
'J6MM6CZ33H577MF6',
'MVVKWJSL5T5FLFF6',
'SF736J2V237PXPB2',
'C98CD9TR8LN95B53',
'HQ8B8S7CLRTH2C52',
'M6B7D2GM7NDLFQF6',
'XB449NSCG6RBMRD3',
'SVCQS9BGV75XQ753',
'BBVT6N5X6RDG5PF6',
'1NX85741R6768482V',
'PRXFQ9MHZZMH8C52',
'KBSHVKLMTGSXG6X3',
'MD3JFXV7Q4SGPSD3',
'HKH4D5QRZP5BCB53',
'NZ6CHS4TV7ZFQ632',
'BZP8VX42Z9DD6FF6',
'HN9HKZVJSGC93NF6',
'TGTWMHQWLDWX2PD3',
'R3WGZTXQ74M9BZW3',
'G6STKQTNLR9LV3X3',
'G9H6PSQR4SN4Q4C2',
'3AN95112A58521917',
'GD4TVG865DVPV832',
'6HG11314DC7667543',
'Z6V97QGCTXFHWSD3',
'4TL83952F8943484P',
'M4Z6628SXQ8PJHF6',
'KS4FX28VZ3N3CH52',
'R7KJ23KZXQ683VB2',
'R4RH582JSGC93NF6',
'J66PGHXD3WMBCB53',
'6GV30493WD081474N',
'38U97554D11175837',
'R36TST3588FJ8GF6',
'B7CBZKV85VPFW9X3',
'J79ZFQ6XCCDD3SD3',
'BGSGSNGV2XN83VB2',
'VVNG767B8JKHBFG2',
'CGWN5GTLT28G5PF6',
'Z5C83V6X65S4KH52',
'G39VZH2HXS3J5TD3',
'GS4R8PTHCRCPXPB2',
'BL8KGJ36ZLMWP9F6',
'B83NLR4GHT65RLF6',
'TX9G8MXD3FKKFXB2',
'KFKP3HWLFN3FZB53',
'VX3F9BKV6Q958CF6',
'B8DQH8NS9F3DHK52',
'HK724NZCV8XSD5X3',
'7AE84539552710437',
'M32VQ9PFQQX67MF6',
'ZB3NFJZDH3W7NHD3',
'VDJTKB3BHKXMMM52',
'GT4534GP4S2NPPG2',
'HJPRW9MZKD63LWW3',
'BQKH343NV7ZFQ632',
'B5ZGPZNGSFQCXDF6',
'ZHTFJ2VH56P5Q4C2',
'L2K4WDK6F8VDW9X3',
'SZW2B2SH73NVM232',
'WLGVCXDMFTZMMCX3',
'DQXTBWSL53Q257X3',
'ZD7XTBWFQMGQ9RF6',
'JC3JQ4NF4XXR65X3',
'NFMZ2ZM7LK3MFQF6',
'BHGT3HZHKHSFQ632',
'KZBBHRM2TGTNTQF6',
'C3G8SB5K9VWNTM52',
'2RS87794NG535133U',
'VDXKW3KQH69778X3',
'V4CTNBCRVBHVM232',
'5DY08927NY891822V',
'BZ33TVTZ5BNVH9F6',
'WHFWPZGBKNN97F32',
'S5GSTPX24FC8ZQD3',
'14J32248NK432813B',
'73S75611L17998521',
'C4QHDKBK596XJ753',
'TS7S52CX5Q29W953',
'B5GJV6DXDRNXGKF6',
'PQTSSB7CM9MTK2C2',
'K23R96CF2KDKGGF6',
'RJG725D376P5Q4C2',
'N647FHDDLF8JBFG2',
'XF63QBLFF6TR8HG2',
'JVQL3V8JLXKJ8GF6',
'NLH5PPW4WMRSK2C2',
'3WH23677BY0110628',
'J9834HLK8TVLFQF6',
'R9GH4N7V9FHGCBX3',
'RTN6JRM376LTKGF3',
'MW24XX42Z9DD6FF6',
'HP3DKSQJ3DVPV832',
'08E56855DF651861Y',
'ZQSRM2LJ8HNFFC53',
'G7ZLVRLKTGSXG6X3',
'HCDBTHV7SDMRGHG2',
'NQNDW7BHKJPF7C53',
'ZGFZSVKJB7F8MMF6',
'FDBKCH9478JQ4932',
'Q97CJBKHZW24J4C2',
'TSKQPPM2LZNT5SF6',
'RW27C4MPQ9CTF232',
'JV5LFMLDTVF778X3',
'SFQV24KKC29L8LD3',
'XKGCN6LGNJCWRP52',
'HH42SMPC2DVJGGF6',
'XVZBZNRM3H577MF6',
'SWLSNJV48KTF5BX3',
'PX5T67MRSD6RWFF3',
'NJH7X87BBC9Z2C32',
'PDMPZZH2XTQ5GCF6',
'T7X9JSPQDQBR8653',
'NLCBSBJLBVKJBFG2',
'BMR3H4M62XN83VB2',
'Q5QRWT4NCH5WLND3',
'D3RHWWCV55S4KH52',
'ZJWL6ZPXQ7QDZB53',
'LWJF5J5SLGPW8KF6',
'GQ9SDHC9PJZ3CH52',
'LMGBV4G8N766VZD3',
'SCH7RBXLJ3X7FMF6',
'CJ52VC72GSSWJ753',
'XBX6S7ZC6R9CKB53',
'M6LJ34MZMC7HCPF6',
'L2W8VZ8M9VKJBFG2',
'JJBSDWPZ73NVM232',
'GF4V93J4GRNXGKF6',
'C6ZSPQPSKFGTLJF6',
'ZGTB9KGG43JXGKF6',
'SDPFHGJWDN3FZB53',
'FG7X9HSGP8TR6JF6',
'H48FWFP94DVPV832',
'WTNC2J8PPMHJ8C52',
'GGG6D5MQMF8JBFG2',
'WMJ9H8NDL5GDPNF6',
'QCHLXR953GX542F3',
'ZVBTL8FLS25CKB53',
'M4LNZWG9H8HSC2C2',
'T24D5WF44BXSCGF3',
'C38PDNPXJH33CLF6',
'GJ9CL3523XBMMCX3',
'L44WJMFGWDMXQ753',
'PZWJWLPV8NJC6FF6',
'VVKTBWBJ6F2LNGF6',
'L7ZH7QMZ25K4J4C2',
'XFJNKNTT6L5FQ632',
'FFB7RC2C2PV7P953',
'HDLR2F8W4JCKG3X3',
'R8XWFW3C6V5VM232',
'LGCWR743XS9TK2C2',
'H4F7RZGPR4HSDJF6',
'WTLFPKZ65525RLF6',
'ZJZTQQKX2XGWLND3',
'ZW3K9ZKQX5RNTM52',
'VCPHWRM7LXPCHK52',
'B66298GV7SR9JDF6',
'VNSVKVW3QGXP9RF6',
'V7XFDDSGP8TR6JF6',
'T56D85FVWLGGL2X3',
'DCSQ9KK4JB2QQ4X3',
'NXX2TKD33HLL8LD3',
'WWZ527DK4PSSC2C2',
'QK22563FLSQJ7XB2',
'KDJH5JBFTFJ5Q4C2',
'G8H8JSXPKT6B7F32',
'H2N2NWTN283KFXB2',
'P28H6SKH2GZBFF32',
'K93ZMZ4RR35M6553',
'NBRD3PMQBF5MV3X3',
'SRDS746HJ2HSGHG2',
'3JA59276KF423533X',
'6S566897D97344849',
'N2XDPZCDLF8JBFG2',
'WD925T5CF67XRP52',
'LNP5V2QZFQNTF232',
'NWDH95C659L678X3',
'T7GRXPWJWDQ3CLF6',
'GXBJNSZ96BJ5RLF6',
'THQJSLRNMPJC3SD3',
'RXXCH2F7BDVJ83X3',
'S7HTK586N7HLX453',
'G85TXKSBQ3KTS5X3',
'V2HCT57VCTNGFC53',
'BVTTF37493PH2C52',
'D5793JZZ9WLDL3F3',
'MV42V9SMFPT9SKF3',
'D9R75XPXBSRGX632',
'9U4135779J2425028',
'BXRDDX9TSDM4KH52',
'2MR078745G465673X',
'JBRGDZXZR4L29C32',
'C2K9LBG2B65GL2X3',
'SVXRL7KBTB5SG653',
'JKBW5WR2226R8653',
'DFZCNJPL5VPFW9X3',
'V3GFQ25WX3MGX632',
'L855B3ZQ2KJDPK52',
'HTGX7FLWDN3FZB53',
'C7538GC4TB5SG653',
'ZSTDDK25QPHR8HG2',
'D3XNV3S55S2NPPG2',
'XNPFZJRMRGTTF232',
'C4VXL9P8MWK8MMF6',
'TJGGMPM2LZNT5SF6',
'QPS5TRVK33JXGKF6',
'NBFCCRW6TBTP4932',
'BBJN5SVWHB3G5PF6',
'JZSSMXD2DRCPXPB2',
'ZZWMBHVZPLCFDFF6',
'ZSRB57SSS95NMM52',
'JH69HS2V9VWNTM52',
'DBV9MJDVD67XRP52',
'13G17081SU6411329',
'V6DVGDGBVNTQ4932',
'ZZ2LV68T7KF7H953',
'JPL9MZ4MPMHJ8C52',
'TCLG29RQBSRGX632',
'QFK5PNXT9VKJBFG2',
'HKJB668HV6KNQPB2',
'V3K6J3QJX39DRB53',
'JP3TML5S8MWCRB53',
'JFN46HRVRD3QMVD3',
'HJ9HZ5P9KXKJ8GF6',
'GPL6K3BJ2G2MPPG2',
'XL7Z62TSDRNXGKF6',
'ZHGSP3SXSB5SG653',
'LJJK9XCCMB46GCF6',
'BBTHWDKJJHJQQ4X3',
'W5J3PCV7RK6NMCX3',
'CX22S735326R8653',
'XXC75RN26KPMQPB2',
'SF5SC6H284M9BZW3',
'R5FW37QBHQNNTQF6',
'XQNFFFRSB43KGGF6',
'GL65MBLZJKJ2HPD3',
'FJZ55D6VZDZTCSF6',
'H9R6K4H3M6PGCPF6',
'CQTRTRTX44NWJ753',
'G9S3X3956Z65Q4C2',
'ZV88MDW96LVF5PF6',
'LBTNLDGGSPW678X3',
'5VH34234ME4768318',
'S5NDH5C5HXNKNGF6',
'W4L73QSQVZ2NQPB2',
'T6JM7BFMGFDFW9X3',
'XS67P6DLKZNT5SF6',
'SVP5W4D4V3DF7C53',
'F5W5B8C5HXNKNGF6',
'WL7PZ6R5RR469953',
'PCMTQ5KBKNN97F32',
'BPPNQQ97VK3CTRD3',
'WTL63DSGS9CNTQF6',
'4UR13616EE212850L',
'V9WSPVRC2DVJGGF6',
'NNWWVQMG5KJ8W953',
'LK4R35CNBSRGX632',
'K7CWDHHGSPW678X3',
'NW7GJ7H4KS6T5SF6',
'BBKVGXZNNXP7P953',
'GCPH65HGSPW678X3',
'VM5CNLRS3742W6X3',
'KT2F65G6KNGXQ753',
'CLV4WR2PNXP7P953',
'MNBXM9WN283KFXB2',
'SNR8S6D6FL6XGKF6',
'NG7NZ8H4KS6T5SF6',
'Z76CWZKL852B7F32',
'NSXHDZ9W5G4778X3',
'J8N3FQG3M6PGCPF6',
'HTDM4ZQ5RR469953',
'XXW7DSHHB7KZCBF6',
'LRN7Q2XDTGTNTQF6',
'PV94S3FHJDTNWPG2',
'KCCZ46K5QJW9MRD3',
'DZDRBHZKBXMTCSF6',
'PX6ZD7ZVL9MTK2C2',
'J7X2NWNKSFHHMC53',
'JT8WL96H5TMSDJF6',
'D2T9V5FTLSQJ7XB2',
'VFVC74GC5BHFD2X3',
'B5JRKKGQ7NDLFQF6',
'FM9RXWBX33JXGKF6',
'0US97829YX4868737',
'GV6XMML3CWJJGGF6',
'JP6TD49S2LHVCSF6',
'QHK9V4TNNCH3CH52',
'ZQRMWXFVS3DF7C53',
'XQGTW9N777ZXG6X3',
'MWK2BLVX5J4HWSD3',
'XWTP63FSQBCXGKF6',
'NG4H7F7B88KCHK52',
'NWCZR352Z9DD6FF6',
'F245RCHCTXFHWSD3',
'5R411513MF9754700',
'C5MN7W64TKC5RLF6',
'JQFLBZWJWDQ3CLF6',
'KLFV4R6P9KGTK2C2',
'HRLFG5SSCJNR6JF6',
'8TM65573J9859482C',
'D3VW6KN7LK3MFQF6',
'WFLWVJ2N5TQ29C32',
'RKTVH9ZP85W3CLF6',
'DCCXPMPC2DVJGGF6',
'BBBC7C7NFMMMZTD3',
'B9TB2MZQG8JKG3X3',
'QP5KPB4H8KF7H953',
'C258VDZKT28G5PF6',
'HZZ4G78V88FJ8GF6',
'BP72SVWQHQNNTQF6',
'FFQKJ2XVQJWKZBX3',
'KGCHXPPSHJH99VB2',
'RQF97Z5P9KGTK2C2',
'M89PMRHW5VPFW9X3',
'SXB2VX42Z9DD6FF6',
'R7KDZ774TKC5RLF6',
'NXDBPSCFTFJ5Q4C2',
'LCQL4KN6NWK8MMF6',
'G4JVQSHZBHBLX453',
'RDKKBCGF6F2LNGF6',
'Z2865RJ295SXQ753',
'LMKGK59MQNJCSJX3',
'GHRMXZXSL4WH5TD3',
'N6GTXXHKSKC5RLF6',
'LXBG6B5BFW9Q9RF6',
'GH7H7Z5BFW9Q9RF6',
'JTHVWLTFG8G69953',
'ZNFC83V2LGDNQPB2',
'BNXWR8FW3H4HS2X3',
'MC9LSF8DPPSBFF32',
'L7CRTMW99BWNJHF6',
'WC6L5MTT6L5FQ632',
'V48GZMP6Q35M6553',
'F4R3WPWVQJWKZBX3',
'RCHF9W7KH2HSGHG2',
'XFP9NMBVQBCXGKF6',
'L6ZZZSFBKTXXQ753',
'P52NG4M3MVB6ZLF6',
'LLCK735BTDM4KH52',
'XQ5688WSKRTH2C52',
'QHBSWNKCBM2HCPF6',
'N57QKFLL852B7F32',
'NHR86R6P9KGTK2C2',
'KSH9MHHG43JXGKF6',
'T3K86S49V9RH4FG2',
'L55XQSQ3PTBVCSF6',
'RRRXQZCRVBHVM232',
'S4QH6XRP4GLBMRD3',
'G967SN3GKHSFQ632',
'MTT4RSCGCF38P953',
'P6L732VWKXNQ4932',
'SBKN3286PTBVCSF6',
'HZ7K972625JGFC53',
'GM55P4V2LGDNQPB2',
'CXCXL362Z9DD6FF6',
'NCCRV3N6NWK8MMF6',
'Q3WDNTTHQNJCSJX3',
'R9LH3SVQHQNNTQF6',
'L6MKJ2PD7RCQQ4X3',
'JQNLSCMD89JT5SF6',
'B3BZZ7W8SJZWJ753',
'Z5PSRTCFPQFL8LD3',
'KDJ7W6MZMC7HCPF6',
'8H911224F1540291R',
'W4W9S4Q2XTLDPNF6',
'HRHKVKXDQN68ZQD3',
'D2292LFPB2P5W3F6',
'XFK5PQ8SWRSFHSD3',
'QCJD6P86PTBVCSF6',
'ZBL3RBX8SJZWJ753',
'F8HQ9CQQSZT8W953',
'TW6MKNWJWDQ3CLF6',
'T4BKZ69MQNJCSJX3',
'JD2FCJ8BQLWTPWD3',
'NDQV7H7HP7J83VB2',
'HD2T3T3V5J4HWSD3',
'KP5KN4FZC6GL6553',
'ZDMFMPMZL8MB8LF3',
'QD4KS4DCDTHR3WD3',
'KTBJLZPBMR77P953',
'S3N29B2M7JWMTM52',
'JRLZKWTB4FPCSJX3',
'PX598QC36J4HWSD3',
'GBJ959S638T7ZQD3',
'HC9W8D27P588P953',
'LBRHMQL44BXSCGF3',
'Z3F9CDNRCZBR8HG2',
'FWMPTKK4BP3JBFG2',
'6AG11335RU7293544',
'W8JGJDSLSB5SG653',
'VZWQX3MQMF8JBFG2',
'DQZ8VMFLFQNTF232',
'T74QN9RSX6VW8KF6',
'HTBFV58BTZ458CF6',
'Q7JBKQMDDV6FDFF6',
'JVX34LWCVDQ3CLF6',
'KG2345TTV6KNQPB2',
'F9CNBFDBVCG9BZW3',
'K74MZQR2VQZXZP52',
'XFDC9L9NQLWTPWD3',
'G9VXD983VH8J8GF6',
'V46GH8DWCKWTSJF6',
'VJL4GSF9S5KFW9X3',
'G7JPG756MHXZVKF6',
'ZZ4XGJH46G4778X3',
'FFS348GP7LN95B53',
'FBJV74WP22MP9RF6',
'JFFC627ZCSCQPFF3',
'B873NR63DT967MF6',
'ZRWXTMC794LJ7XB2',
'DZGW72ZQ9VWNTM52',
'JR8X2M6MZQ683VB2',
'WL8VW23WJVQTKGF3',
'HS2XN588STXDQ632',
'61W5704429524440P',
'QPX6GRLNRXL69953',
'Q6QDXL2H4DL2W6X3',
'KFB759G3WNTQ4932',
'H6WVLJTZZ4MKZPF6',
'SQMBRKNHBX92WKF6',
'PQ9Q5RC7Z4LK8LD3',
'NSN6NTK3J7CNWPG2',
'BNRP25T5WQKCTRD3',
'XP9BX9N777ZXG6X3',
'HD7LG2F3SFHHMC53',
'VP7T4BJCX6VW8KF6',
'K4KH9P6XQMGQ9RF6',
'WQQRZ2LT2HXP3DX3',
'BBTFT58R8KGTK2C2',
'D3WFTNG8FL6XGKF6',
'KGKNJQ4M85VCXZW3',
'JWXHMCRPJM8J7XB2',
'XXGWQ87TGLXRG653',
'PVXZWSFGCGBBCB53',
'TT2MWBWT79JT5SF6',
'XRX5Z7PZRFKVS5X3',
'VL6KFMDPGJH99VB2',
'64035700000000000',
'1S257540V4966820X',
'N59SKHJ76BJ5RLF6',
'FD4H89D2LK3MFQF6',
'X6VK8NTG6SJF7C53',
'QRNLW9VRVSTV8KF6',
'GXPHDJSGSCXDQJD3',
'Q8ZFKMTVHJH99VB2',
'DQ54X6Q2CVKJBFG2',
'JPG5DMHLKZNT5SF6',
'NMD46LQD6Q29W953',
'70306792AJ739032P',
'4B432685GD431894G',
'NSWBKXKWMB46GCF6',
'KMZH2TBT7B8CTRD3',
'KDLMLKTLGSSWJ753',
'P9GWKKHX93NP3RF6',
'SBPCC34FSXNKG3X3',
'ZRJ7DDSGP8TR6JF6',
'JWKC5DRD6Q29W953',
'PSKSSCR9QKV4J4C2',
'H72CRP953XN83VB2',
'G52H6D387RDG5PF6',
'PLPPRZWQ4DL2W6X3',
'GGTXTN69V9RH4FG2',
'XX422RTCG6RBMRD3',
'X7GQJNLK6PSSC2C2',
'R95D5CR2CVKJBFG2',
'VN2CQ7387RDG5PF6',
'VKRP8MCS6QTKKTD3',
'ZR7MT9MZMC7HCPF6',
'W3ZFPGLK6PSSC2C2',
'B6BCNTV3SXL69953',
'TWQWQQ6XQMGQ9RF6',
'P342PW5BHKW29C32',
'R2XMSRPSZMQ8MMF6',
'LVHLCMLMN7HLX453',
'C5J7WGJ3J57LNGF6',
'DDH6SDVXTWTQXHF6',
'V2ZZH9LKHLVFW9X3',
'J7R3DFMDTVF778X3',
'F9XTXNH3J57LNGF6',
'R8LQSTNZMC7HCPF6',
'B6M92J5QCZNWRP52',
'GMKT3K49BM2HCPF6',
'QG24MX6XQMGQ9RF6',
'MQCMLTSGP8TR6JF6',
'WDQBBX4S676GLFF6',
'5E860131HX497211X',
'H38SDTJBKSQJ7XB2',
'G9HN74CMKTGKX453',
'WFKRM5PQSZT8W953',
'XVXQS2KF76ZDHSD3',
'FQSDFS3ZCF38P953',
'CV8PX58ZXRHR6JF6',
'DTBTBVV9TSJMQPB2',
'SMZKHC3SBDD5GXW3',
'8ET7238303814031R',
'8ME3567824220623X',
'R22JVWHSX6VW8KF6',
'PJX49NSGF6TR8HG2',
'NXLG9554W7ZFQ632',
'PR442BT242JTF232',
'J73Z6T9NCKSQ8653',
'S57RGK3KDCPMPPG2',
'DXQ58GLJ83PH2C52',
'RQT9FQCLRF3H23X3',
'CTTTC7S988JQ4932',
'WXHW5TQ9QKV4J4C2',
'RLBZPDZJ6R9CKB53',
'DRNQMRLC9VWNTM52',
'QLGCZMSWJ8XSLJF6',
'BMWDG3QNBDVJ83X3',
'MJZNTGRTQLWTPWD3',
'WQN9DK8CP6HBCB53',
'CW25CFXZ8LN95B53',
'BQ2R5L3XLCP3KLF6',
'R368872T6W49BZW3',
'HKGHTLRPD8DSPDX3',
'10R986357N412460J',
'QJ6KQC5TCHC83VB2',
'VHT9M2SH33JXGKF6',
'77021696P22978508',
'Q26KVW8LT95NMM52',
'PLH6PD8Q6RDG5PF6',
'KLX9WRVFQMGQ9RF6',
'01X596491F642262D',
'08Y77688PW944482S',
'TPMH4RDDXFXFL2X3',
'47R321129U286211H',
'9LH639853B179833E',
'G83SGBN6H9P8W953',
'0B1115913C240731M',
'L4SFBDP62SHR6JF6',
'H9NDHB8KCSRGX632',
'JHJT3D23W85BJDF6',
'LZJVDQFZD67MPPG2',
'GX9XNZH7DTP4J4C2',
'XN8JCPT4KJB5ZLF6',
'W6724RJKHLVFW9X3',
'ZWP558FPH89HWSD3',
'JZHC44HHPR2SPDX3',
'CXMLR2WTBN6GFC53',
'ZJ2QW34CWN85GCF6',
'QRB773K5QJW9MRD3',
'KKK4PPDPM2ZKX453',
'HWPTPJL493PH2C52',
'CJZFJW9SG43QV832',
'ZWV56TQ9QKV4J4C2',
'X54TV77CM9MTK2C2',
'C5T95DKBZLGGL2X3',
'P7JWL8C8LBGZVKF6',
'QT4XXS67LT6B7F32',
'Q5NNLSGJ2Z423C32',
'Q9JRPQ5MV6WH7XB2',
'SG8DD26LBBT73VB2',
'NSWX2N8K5L5FQ632',
'3DJ95171BF429934C',
'TD5SLML3CWJJGGF6',
'98E862201W5969145',
'S2KJQKZHLDPC6FF6',
'ZJ3G9MCRWPCGCPF6',
'GJ4GNZBQ7NNNWPG2',
'GZ2X3K8VP5TWGKF6',
'CZT3JNS88XD3CLF6',
'L7S495852B4L8LD3',
'FS5T78JZ6PLHWSD3',
'QCCL4XF5WQKCTRD3',
'BHJT7TRDV8VF7C53',
'ZZDW9554W7ZFQ632',
'WMPG86DDJ2HSGHG2',
'VQ9ZNWDF83NVM232',
'MJTMJKHSBTNGFC53',
'C65QNWNKSFHHMC53',
'LR5845QH9ZTQTVD3',
'XGKSNBBXPKV4J4C2',
'CL4PKJH3J57LNGF6',
'BZTQWJ3SCTNGFC53',
'N3JFM65TBTQK8LD3',
'V2K2FB5MP588P953',
'N8MK8VJ8ZLZQ6JF6',
'QK9RPR3ZBLZGCBX3',
'R42XGDX44MG642F3',
'F6QGRPSV9P3JBFG2',
'KMMGPZGBKNN97F32',
'L9DXQ3852B4L8LD3',
'SVMKTDXDT49H4FG2',
'ZQJFTHBJ8F9DPNF6',
'JWHPKQNJ5KJ8W953',
'D2LV3P47GCXL6553',
'FPRVBL3N375Q9RF6',
'48G76608293693350',
'WH8QZ2KCF67XRP52',
'QVW43D25P588P953',
'Z39PHXVQ4DL2W6X3',
'ZDTPW43WCCDD3SD3',
'C5QZ9WCH4JBSC2C2',
'K4TFNC5SN6WJZBX3',
'QLFM6ZTQBVKJBFG2',
'ZBQ2Q7DK65CRWFF3',
'LXXXKF5P2XN83VB2',
'PGW6XJRH925THWD3',
'WWWKB422QGXP9RF6',
'ZCF7STWS6Q958CF6',
'HBKJPNBRCV6FDFF6',
'D3HWN5Q4NC7HCPF6',
'PTZQ3RNCW5K5F9F3',
'PQCS4XQKLC228GX3',
'W9ZB3PJLT8VF7C53',
'WMPMG2VKDDMGPSD3',
'SF6WXBDFPQFL8LD3',
'NQWTBKJ295SXQ753',
'XTMVTSBFTFJ5Q4C2',
'LMFNBD3N5TQ29C32',
'NFMFFGTKDDMGPSD3',
'JCKWVKLD89JT5SF6',
'KZPCQQCX75SXQ753',
'H8PRN9KNMV9GL2X3',
'S2MNJ86BFW9Q9RF6',
'KTKQRHQ9PW9GPSD3',
'LJLHWVP724MGX632',
'LR4GXFFM5JBSC2C2',
'GCS7D5N6NWK8MMF6',
'FD6LZGQ2XTLDPNF6',
'J5VTH2QQSZT8W953',
'P2RVLBLZJKJ2HPD3',
'LBGN225ZCF38P953',
'GS3F9RXJWDQ3CLF6',
'WQMWPDQQSZT8W953',
'7AK50829R07557912',
'CR3XWM52Z9DD6FF6',
'MHS4J7QZ8LN95B53',
'SMJ3XDJ295SXQ753',
'7BA171403X906330F',
'GZG3G3X8SJZWJ753',
'KDBZXV86PTBVCSF6',
'KG5LT8965DVPV832',
'JFVMWFJ8ZZMH8C52',
'FGWJKWTKDDMGPSD3',
'P2ZG8XG7WS87H953',
'GDX55WC98NNNWPG2',
'T3HQ6KGF6F2LNGF6',
'PVGGMPV4RMF2W6X3',
'XLX5B4ZQG8JKG3X3',
'PBQXH6D794LJ7XB2',
'JPWVDMLDTVF778X3',
'ZPRQQC77KWDH5TD3',
'G65F2JG3P94VSJF6',
'LFJ66LNNSK5SGHG2',
'FBMJD3Q2KHJQQ4X3',
'WP4Z2J7284M9BZW3',
'L797VPVRKTXXQ753',
'P6488H8C9666ZLF6',
'L6MPQ8ZD3KDKGGF6',
'K5MWQDFLHB3G5PF6',
'44C73673TP592422V',
'TR9VD5B2HNG4KH52',
'00599922WE9851130',
'BR3RNS7WJT6B7F32',
'BV4TWXJ3244PFVD3',
'FQ2JTB7CM9MTK2C2',
'XQZBG6VVKNK3CLF6',
'PNDHMD2FSXNKG3X3',
'J3CS9B4CF67XRP52',
'CF46PWRRH2TXZP52',
'FMQQX83KLLBP3RF6',
'S89C9FB8Q6BMZTD3',
'VSTLZKKCBM2HCPF6',
'ZQ9T4ZC7MWJJ83X3',
'KNNHTGNN84LJ7XB2',
'W3CKB6DGCGBBCB53',
'X39J93Q888ZXKXD3',
'JW8LCQX2KR7CHK52',
'VWQ8T7N54Z5HMC53',
'CXVZLXCDLTKNJHF6',
'4VD12061858541628',
'53089784EG180360F',
'89S03567FE790124H',
'MRMXJ9FN85W3CLF6',
'JGJ75K76H9P8W953',
'X52B4Q4SVJCT5SF6',
'T4M66GNM7PQ8NHD3',
'LZ9VL5GPHQMXG6X3',
'C3LN55JV9LW5GCF6',
'LZF4PJ2LKJX9JDF6',
'B52ZKJ4DZJM9CB53',
'JWQ99W63NWK8MMF6',
'VXLS4MXV596XJ753',
'Z9PMNFH3J57LNGF6',
'ZH445K8VP5TWGKF6',
'N29TTXLTVS9TK2C2',
'NDD37KSFHKBFLFF6',
'H4CCCTMR5T79Q2F3',
'CRH5MQGXWL4Z2C32',
'X487ZHDG375Q9RF6',
'D65DK7KKV52H2C52',
'G9D8H8F74RR3KH52',
'WLN4B2LD923ZCBF6',
'RCKKMCMRH57LNGF6',
'KCDQZCWH2WMBCB53',
'CSMDGKQGRJ435LF6',
'JRNZQ9NXB43KGGF6',
'H9VTLHTD85FNJHF6',
'HVXSQ6B55S9M6553',
'3YW498949N983413W',
'31L60573K2614053W',
'BC5RFF7JTB5SG653',
'RCSLJKWHB7KZCBF6',
'JMWZ8RP6P588P953',
'SST48559RR6KG3X3',
'XFZNF9ZZX39DRB53',
'66J5511464501550J',
'WB39QLHMQNJCSJX3',
'TCJS9MZPJJRSD5X3',
'LGLC2B5VTVLNBHF6',
'L9N6X43WCCDD3SD3',
'P2VD8NSCG6RBMRD3',
'BQT6C3KDVWTQXHF6',
'KDP9SN9Z73NVM232',
'SKZNJRW3S4L29C32',
'L29GPTK4DSS3CH52',
'ZVT3X9N777ZXG6X3',
'VWDSTND33HLL8LD3',
'RGD7BDP62SHR6JF6',
'S88KWTMJ7SJF7C53',
'FTJDR56CX6PG5BX3',
'G7F57GNNS9RH4FG2',
'7D537020H5444994K',
'07T47440SP0246726',
'3D746261HE7467711',
'QZK5P8PBRZRVCFX3',
'DMX8G39FR3699VB2',
'ZG6GF3DV55S4KH52',
'7FH22412TH999145X',
'RTZRVG6NB2KD6FF6',
'P5S3QSDF83NVM232',
'T623SM5HTB5SG653',
'KRC25RPLQW329C32',
'QF8MVR34LV9GL2X3',
'ND6TXZ8RHPC5Q4C2',
'0B354975TR903501H',
'RJSPWKNDZ5FJBFG2',
'ZBCS5T5QCZMBFF32',
'NVQTQ6GRPQX67MF6',
'7ER0510264206974G',
'BQWMG6W7F67XRP52',
'SMT3RZG7RWJ8Q2F3',
'1YS476994Y938014R',
'M7DBXH4F92W7MMF6',
'FVMTQ4L6P2BR65X3',
'XLD9ZJ4CX3V3KLF6',
'MV785X93X39DRB53',
'X94V7HCF83PH2C52',
'KBKHMX336SB99VB2',
'JD5TWD8M66P5Q4C2',
'D4F8L94WNNLRM8F6',
'RXF98J6XJWLSK2C2',
'HWCDBG7JMVGWP9F6',
'DCLNLV73SFHHMC53',
'GLNZPSC9QLWTPWD3',
'3NS42381H7283724T',
'VV887M65VBJH2C52',
'VBSWSXZP9T2D6FF6',
'QP2K5VVGJLR45QD3',
'DSQNG2DGPLL59953',
'SFX433FP2KJDPK52',
'V9X9ZZ8RHPC5Q4C2',
'TWC97Q4GHT65RLF6',
'XFNSZDFPPMPPV832',
'83F84090H9937693L',
'R68DTX6JRMPPV832',
'S5WZ5JTLGSSWJ753',
'1XR79201TG976615S',
'5TV29212EW374102U',
'7VA3227663353334L',
'G53ZQG77KWDH5TD3',
'7RC82206HA7792123',
'WLCZGWNM9G8NFVD3',
'XKX46LZFQLWTPWD3',
'RJBHT728KV5FHSD3',
'73586579X5039774H',
'J46X4VBRH89HWSD3',
'GHZCXRMCNDGKZPF6',
'H67CPGF897DJ83X3',
'Z4VMTF83TTFM7VD3',
'FFS97KGSSTNPQ4X3',
'TF5DHB5GRZT8W953',
'GSXHH8NDL5GDPNF6',
'VDVWKMZKLFLNTM52',
'BQ364GBJNCGGX632',
'S2PNTJ7R2KDKGGF6',
'Z4XFFB7PK7TFFC53',
'CP7G5LM58KLCL3F3',
'GZ6229QSFN3FZB53',
'T5PLCG2T8QCG5BX3',
'ZQ9S5ZCTCQBR8653',
'LH3GMGQ98P9TLJF6',
'FW8KHTSD85FNJHF6',
'ZGV87BHKZMQ8MMF6',
'HX4LFHHMTNZ85B53',
'CHM6NV43ZM8TSJF6',
'V5BDXWPBBHFPJHF6',
'QWLGRP5DKTGKX453',
'JZ4DC6RC8QD6JVF6',
'JGLSH5FCZ2KJFXB2',
'H3LWRZKL852B7F32',
'WSSPRKWNG5KF5PF6',
'DMLK5ZJKKWLSK2C2',
'8LM58784CK2742929',
'VMPRB4KLKZNT5SF6',
'K9WCBGKHCTNGFC53',
'LMK7KHS3QZTDPK52',
'JT9J42VRG69778X3',
'WGC87LWCBZMBFF32',
'MMQWSLV3TFBS9WD3',
'WHNNZCPH9MWCRB53',
'3EJ40427S6479204U',
'LNNFZH57TDM4KH52',
'CZ35M53Q684BDJX3',
'DTJ8KBZ2LTGKX453',
'FGJSGPHK9JZFHSD3',
'WTDWVTH2PNDKFXB2',
'G9MQ5C8VC29L8LD3',
'G47S2GTW3J969953',
'DK7ZJMTVHJH99VB2',
'JRRM3R8DF7D8Q2F3',
'N3HMVXFHBHKG96F6',
'99H07257989556020',
'7X696754LE285212H',
'JXXMGNB3X6VW8KF6',
'Z5XQ64JMHQNNTQF6',
'PC7RWMK9KXKJ8GF6',
'VH8LNMHTNPP4KLF6',
'CMFFPBRSHHGCKB53',
'RDBJR7SRXQ683VB2',
'LX2G69PDGLVFW9X3',
'J4B3DQSR74M9BZW3',
'B3H7M38R9VWNTM52',
'H9M7WSS88P9TLJF6',
'ZFRCVVBTBSRGX632',
'TR7QD95C8F9DPNF6',
'Q2QXCX8KBSRGX632',
'HZD9WRDLVNTQ4932',
'BQSCWBX8W6PG5BX3',
'DXXTDWSW9WZ7VXW3',
'NB2KC4WFX39DRB53',
'Q6KMMNJRKTXXQ753',
'F88FQQ55DQBR8653',
'MKSG92DLKZNT5SF6',
'C53HHHP8KTXXQ753',
'RQJ4W5NHB7KZCBF6',
'QJ9S5WFNFRNXGKF6',
'NRJLB3T6GNVPMVD3',
'D4RP6LMRT3DF7C53',
'SQBG3KV9CT967MF6',
'DVMDVW6ZXKGVLND3',
'7SD35359N6787002G',
'SSP56RC3VVLNBHF6',
'P4VHPQSDHHGCKB53',
'712668201B5410728',
'KWGNPWDF83NVM232',
'CPZS5HPTW98MFQF6',
'HGTWPKL47RDG5PF6',
'CXK5XK4W7K2W8KF6',
'D6GG752S7RMFQ632',
'WKVQFT6X56P5Q4C2',
'X6GLXKPNS3699VB2',
'0P608401S6467583R',
'9RJ95254AU954513G',
'8NB796034W959383E',
'QGZH2KTN85W3CLF6',
'DHVPLGNV78FJ8GF6',
'VSLRLP4LDDMGPSD3',
'JFX6SKJJ5R9CKB53',
'PTLH9GLLJHJQQ4X3',
'SBBL8GGMSVTD7C53',
'M8SLZCG2W8VF7C53',
'MLN7Q2XDTGTNTQF6',
'Q66SDB5MP588P953',
'PWK552LSL9MTK2C2',
'WTR3RL4ZX39DRB53',
'GRPPLFRJQNJCSJX3',
'W72ZXL63TBTP4932',
'ZRRHH7CJGPDVS5X3',
'J5C6PK6PRWQG4FG2',
'TQN6Z3Q2CVKJBFG2',
'BXMMSXBSG43QV832',
'K2DCD5VV4S9M6553',
'J7T43H387RDG5PF6',
'ZN3W5P4SCTNGFC53',
'KJQNHTRGFW82W6X3',
'G8L32DG2W8VF7C53',
'X8B5DX7CM9MTK2C2',
'XP5VJHKT8KF7H953',
'ZJK7R7RD6Q29W953',
'MDBB5W3SCTNGFC53',
'M347928SX6VW8KF6',
'RHHLJX994FPCSJX3',
'XGHTRLZ24FC8ZQD3',
'QNRMC2TN9LRDPNF6',
'DKCQ47RTCSRGX632',
'FTBT9LDFTH8J8GF6',
'BBT5NN8Q6BJ5RLF6',
'NQBJ8M4M3FKKFXB2',
'VT7LV8R9QKV4J4C2',
'VKSX749Q6BJ5RLF6',
'D4DG8JH2W8VF7C53',
'FGRPLJQ2KHJQQ4X3',
'P5CHJ4NDTVF778X3',
'ZWD6VC4M3FKKFXB2',
'CL533B3Q684BDJX3',
'TTT229QSZMQ8MMF6',
'JCN88J5MP588P953',
'B8NL75HF83NVM232',
'G37549VGP8TR6JF6',
'F67HN7C94FPCSJX3',
'PD4FS7S438T7ZQD3',
'D3RW34NK6PSSC2C2',
'KBCKV9HWKDTNWPG2',
'KCPJVD49M9MTK2C2',
'QGSKKF7FX2RQM8F6',
'C34G7KJT8KF7H953',
'JM8DZS7NB2KD6FF6',
'K2K8FQRD6Q29W953',
'WXWFJP7RM9FJ5TD3',
'FGDVHMH3J57LNGF6',
'GQXDXDH476LTKGF3',
'H26JLDKKHLVFW9X3',
'GCRZD38XQMGQ9RF6',
'VZNGP2W93KDKGGF6',
'WHJV4RN777ZXG6X3',
'DLBWJPXNW6KNQPB2',
'FF7T9F5MP588P953',
'NXW8JJF376P5Q4C2',
'BCFDPXNZMC7HCPF6',
'WT2GBGHVWLGGL2X3',
'KGVP5NV8GN3FZB53',
'D7LV8W54W7ZFQ632',
'FGV632CJLXKJ8GF6',
'JTZZ3J8NB2KD6FF6',
'LC83FXV7Q4SGPSD3',
'CLSP9NRTCSRGX632',
'HD24VPBG9JZFHSD3',
'NLSHKZ7Q6BJ5RLF6',
'D9D22ZWNW6KNQPB2',
'FRRR5B9Q6BJ5RLF6',
'C3NM75JJ2Z423C32',
'B84ZZRV9CS6WRP52',
'CPKVLJKMN7HLX453',
'CZGV56VGP8TR6JF6',
'ZK6V3D3M3FKKFXB2',
'J5ZK9VV93KDKGGF6',
'D4J3WRTVHJH99VB2',
'JRGFWRXP93NP3RF6',
'C83T5HC9MGPW8KF6',
'VJ8JRND33HLL8LD3',
'ZTK6S6XQ8LN95B53',
'B2CJCDSGP8TR6JF6',
'JGKQJZJD6W5ZNKF6',
'T3PQ2BHK2Z3TPWD3',
'86E81358MR677491V',
'HRTQ673VG9JKKTD3',
'LG74WKXHQMGQ9RF6',
'S96ZXTPLQBCXGKF6',
'KD32649K65CRWFF3',
'P86LQF7QFRNXGKF6',
'DV9GDT792GZBFF32',
'JV9RCD9M2XGWLND3',
'VNPJMP892M7D3SD3',
'XDLGBX7TLVB6ZLF6',
'QG69PTN4VQZXZP52',
'4JY50117P2103000P',
'L26DX4FBDFB42CF6',
'2LF71970NV747600S',
'NJJ6F66GCD32DBF6',
'SFHVG3KD3HMRDJF6',
'SMBT6ZPXQ7QDZB53',
'H8672C97VK3CTRD3',
'C5GVMBS7QLL59953',
'SVQSV2ZZR4L29C32',
'LJQBKJSDN7QG96F6',
'85762247RD487125R',
'NZ6FH4Z6QJZ3CH52',
'LL33DXS6QCL25LF6',
'QB37BHVK9G8NFVD3',
'CCPJFZV6JJPF7C53',
'RZ8PWVKNLTXXQ753',
'TWFWWXDDKJPF7C53',
'JNG23SD794LJ7XB2',
'KMM3HW9SG43QV832',
'MN9NH345BK8J5TD3',
'TPHT3J2625JGFC53',
'FX2Q96Q2XTLDPNF6',
'QBTBTHM6NWK8MMF6',
'JHXJTVLKTGSXG6X3',
'JMWDMKJW5VPFW9X3',
'ML6CFBBCTCTS5SF6',
'RK6DJPRTCB3DZB53',
'QBN82HMKTGSXG6X3',
'QSBZ2LKCBM2HCPF6',
'G37TJJWQHQNNTQF6',
'BR8PMM25PQRP3DX3',
'T9QXN2CFTFJ5Q4C2',
'QLJV2VJ295SXQ753',
'FDWTQPCFTFJ5Q4C2',
'W2T7KWPQSZT8W953',
'NC6JRCRHR3VMPPG2',
'LCVX9CQQSZT8W953',
'C3D35L5PLGJG5BX3',
'WL7LZKSL4XVDZB53',
'FFQQ4XFVN8XZ2C32',
'JQZNK2QQSZT8W953',
'DCQF554ZCF38P953',
'R8JGHGXJWDQ3CLF6',
'L2LQLH35GCXL6553',
'S39N7N4MJKLH4FG2',
'KVZDG7NKTGSXG6X3',
'BM7GM8RSCJNR6JF6',
'QJZ94Q4GHT65RLF6',
'SJ2W8VTFG8G69953',
'FKC3CM7V88FJ8GF6',
'DZKHC422QGXP9RF6',
'7WN57032A36619216',
'9EK44701323252327',
'RT45HJ55TDM4KH52',
'KSC99WWRJR7CHK52',
'75K5945671673463Y',
'RHH228DFRX4RG653',
'CXNS94MPQ9CTF232',
'SFWWJW2RF478W953',
'WNG7BTFNQ4L29C32',
'JNZGF6XLNV2D3SD3',
'35P22233BG142234T',
'KM34WT58WH8J8GF6',
'ZWWL6ZR4P588P953',
'15M93661EU346971P',
'QSRN6CL9G43QV832',
'NM6TC2G4R3VMPPG2',
'MMJ7ZXCNCCJ5SGD3',
'JDZL26S4DX7KFXB2',
'TP7MVKK4BP3JBFG2',
'ZBXXHXKWBM9GX632',
'GNWR63WD5VPFW9X3',
'X77LKXSJH69778X3',
'NC3B78VR5PMTM232',
'TR2MJTG2VS87H953',
'FF3WR6FR2G2MPPG2',
'57X17834EH181121C',
'G88M9CHHQZTDPK52',
'LVJFDLCM7PQ8NHD3',
'LVNNS6KSZV53CH52',
'QH7TC75H4XXR65X3',
'Z3WXLGGP5T79Q2F3',
'R6H57FRDGPDVS5X3',
'HC475SG56PSSC2C2',
'B5F3H2SMRXL69953',
'WL9KL42SV6KNQPB2',
'FHS5R6PZM7HLX453',
'FWFSPLJFV8XSD5X3',
'DJHV6PCD4H577MF6',
'RVDRXT5ZH2TVH9F6',
'NPQHDZCK65CRWFF3',
'VP5R8G7C8KF7H953',
'PSGZ6SZ77RDG5PF6',
'GNVQXXGQKBT95B53',
'RH6SMXQNDDMGPSD3',
'FDK7V3BDJ2HSGHG2',
'HSPLSTKLBWJJGGF6',
'1CV43960K3055030V',
'V3KXS3CFPQFL8LD3',
'4N83800201830602B',
'FNNGFV3QM5JNXPB2',
'4EN74221L0684494V',
'NPJX89QQVZ2NQPB2',
'2UN83811210416513',
'LTXN43RSVFZ5ZLF6',
'DRFQTJLX7LN95B53',
'HVKGXVRGFW82W6X3',
'M93B4SVWHB3G5PF6',
'P7ZRKSCJFW82W6X3',
'RWTVG7DSTCTS5SF6',
'VRZ35VZ3S4L29C32',
'LWF7X2ZRDMBWJ753',
'BML6BRV3TFBS9WD3',
'QN7NBDP62SHR6JF6',
'MQS2V8WB2M7D3SD3',
'C66XJZ6ND67XRP52',
'K9RBQ7KK6HJQXHF6',
'JWBLTW7JKT6B7F32',
'BGRCBHS3RW329C32',
'XCQR3FZ9HRV257X3',
'P3ZWVX42Z9DD6FF6',
'CWCQFBDF6NTZGPD3',
'PP29VPXJHT65RLF6',
'ZB8HWG6NB2KD6FF6',
'ZFBPSCMRWZCW8KF6',
'KD7ZJ2LJ33JXGKF6',
'JM97HK532DVJGGF6',
'W6NK3LPTW98MFQF6',
'GHFRXR686B2QXHF6',
'QWSRZGDRSNTKNGF6',
'GTXRMHP3LD63LWW3',
'K2TTW3F7BDVJ83X3',
'VLG2GJH46G4778X3',
'CT5PF554QBCXGKF6',
'JWKKBT59BDVJ83X3',
'D3B9M5NZ8MWCRB53',
'PFLV2JJ2244PFVD3',
'CRT4PHGBT9RH4FG2',
'0HG790044F3678810',
'CPMTVT58WH8J8GF6',
'XV54W7VVKNK3CLF6',
'QF2FXWMTTBHVM232',
'TV5Q59G2B65GL2X3',
'ZGFR6CPDZXHDDFF6',
'PKWHWVKJB7F8MMF6',
'BKXC277FVWHNWPG2',
'F5PQRRDZGK5XG6X3',
'SZ47DSJ3BFCNXPB2',
'JFBXXS9R3H577MF6',
'S7JXGZHJ5T79Q2F3',
'DW2HGBG9M7HLX453',
'TR42FX52MKRSHWD3',
'LCT8V5NJTB5SG653',
'LFHH5PPG5T79Q2F3',
'Z3K5BKMRPXL69953',
'NSNV63LDB7KZCBF6',
'KQMWLRJWT9RH4FG2',
'V2CV8SXNNXP7P953',
'V5QW6S4MQLWTPWD3',
'NNMLVJQNFPT9SKF3',
'Q9PVD4RC6LVF5PF6',
'LX3W3XF5WQKCTRD3',
'P2TCQ6TKDMDBQZW3',
'L4THBS4R74M9BZW3',
'DHJVS6JMZZMH8C52',
'WSK9ZD5XTVLNBHF6',
'RJ35M53Q684BDJX3',
'325387642D1241503',
'QH8PFH2N5TQ29C32',
'PZVVK8T3KJRSD5X3',
'PMT6VXR55S2NPPG2',
'344064157K4218621',
'VLKM5S5GRX4RG653',
'GT3Q4QNDBVWNTM52',
'P8PNKPJQVJCT5SF6',
'53M25334EF7882035',
'Z7K739LD89JT5SF6',
'S946LBLKLBT95B53',
'11290493EM8460202',
'SCB2Z4RNTQZXZP52',
'3F519513WJ6633835',
'FPTS54LG5KJ8W953',
'WTMK3C97VK3CTRD3',
'DWXK8M7N8L9ZVKF6'
  )
group by all
Having (charged_amount + total_refunded) > 0

-- COMMAND ----------

-- DBTITLE 1,Belgium Subs
select
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
launch_wave,
product_type,
productname,
tier_type,
campaignName,
internalname,
priceplantype,
paymentperiod,
numberOfPeriodsBetweenPayments,
plan_price,
plan_currency,
startDate::date,
direct_affiliateStartDate::date,
nextRenewalDate::date,
nextRetryDate::date,
endDate::date,
cancellationDate::date,
count(distinct globalsubscriptionid) as subscription_count
-- card_provider,
-- card_type,
-- payment_provider,
-- last_charge,
-- last_charge_result,
-- last_result_reason,
-- last_invoice_start_date,
-- last_invoice_end_date,
-- last_invoice_amount,
-- globalsubscriptionid,
-- baseSubscriptionId,
-- previousSubscriptionGlobalId,
-- userid,
from all_subscriptions
where startdate >= '2024-06-11T00:00:00.000+00:00' 
and priceplan_market = 'BELGIUM'
and product_type = 'MAIN'
and provider_type IN ('Direct')
and status IN ('ACTIVE','CANCELED','TERMINATED')
-- and startedwithfreetrial
group by all

-- COMMAND ----------

-- DBTITLE 1,Belgium Transactions
select * from transactions_extract
where priceplan_market = 'BELGIUM'
and Transaction_date >= '2024-06-11T00:00:00.000+00:00' 
and event_type IN ('SUCCESSFUL','FAILED','RETRYING')

-- COMMAND ----------

select
UPPER(payment_method_type),
UPPER(funding_source),
MAX(transaction_Date::date),
count(event_id)
from transactions_extract
where event_type in (
  'SUCCESSFUL',
  'REFUNDED',
  'RETRYING',
  'FAILED'
)
group by all
