-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Global Silver Materialized Views

-- COMMAND ----------

-- DBTITLE 1,Users

Create or replace MATERIALIZED VIEW bolt_finint_int.silver.fs_max_users as

with global_user_entities as (

select record.user.*, 'AMER' as region
from bolt_dcp_prod.bronze.raw_s2s_user_entities

UNION all

select record.user.*, 'LATAM' as region
from bolt_dcp_prod.beam_latam_bronze.raw_s2s_user_entities

UNION all 

select record.user.*, 'EMEA' as region
from bolt_dcp_prod.beam_emea_bronze.raw_s2s_user_entities

)
select *
from global_user_entities
where userid is not null

-- COMMAND ----------

-- DBTITLE 1,Payment Methods

Create or replace MATERIALIZED VIEW bolt_finint_int.silver.fs_paymentmethods as

with global_paymentmethod_entities as (

select tenant, record.paymentMethod.*, 'AMER' as region
from bolt_dcp_prod.bronze.raw_s2s_payment_method_entities

UNION all

select tenant, record.paymentMethod.*, 'LATAM' as region
from bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_method_entities

UNION all 

select tenant, record.paymentMethod.*, 'EMEA' as region
from bolt_dcp_prod.any_emea_bronze.raw_s2s_payment_method_entities

)
select
tenant,
id as paymentMethodId,
realm,
userid,
replace(provider,'PAYMENT_PROVIDER_','') as payment_provider,
type as paymentMethod_type,
UPPER(countryCode) AS countryCode,
providerAgreementReference,

UPPER(cardDetails.cardProvider) as cardDetails_cardProvider,
UPPER(cardDetails.cardPaymentMethod) as cardDetails_cardPaymentMethod,
UPPER(cardDetails.cardIssuingBank) as cardDetails_cardIssuingBank,
UPPER(cardDetails.fundingSource) as cardDetails_fundingSource,
cardDetails.cardSummary as cardDetails_cardSummary,
date_format(from_unixtime(cardDetails.cardExpiry), 'yyyy-MM') as cardDetails_cardExpiry,

UPPER(geoLocation.countryIso) as geoLocation_countryIso,
UPPER(geoLocation.subdivisionIso) as geoLocation_subdivisionIso,
UPPER(geoLocation.city) as geoLocation_city,
geoLocation.postalCode as geoLocation_postalCode,
geoLocation.timeZone as geoLocation_timeZone,
geoLocation.isAnonymized as geoLocation_isAnonymized,

case 
  when currentState.pending is not null then 'PENDING'
  when currentState.valid is not null then 'VALID'
  when currentState.invalid is not null then 'INVALID'
  when currentState.expired is not null then 'EXPIRED'
  when currentState.canceled is not null then 'CANCELED'
  when currentState.deleted is not null then 'DELETED'
  when currentState.timedout is not null then 'TIMED_OUT'
  else 'CREATED'
end as paymentMethod_status,

legacyPaymentMethodId,
providerMerchantAccount,
providerUserReference,
created as record_created_date,
currentState.occurred as record_updated_date

from global_paymentmethod_entities
where id is not null


-- COMMAND ----------

-- DBTITLE 1,Plans
Create or replace MATERIALIZED VIEW bolt_finint_int.silver.fs_max_plans as

select
pp.realm,
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

-- COMMAND ----------

-- DBTITLE 1,Subscriptions
create or replace MATERIALIZED VIEW bolt_finint_int.silver.fs_max_subscriptions as

with global_subscription_entities as (

select tenant, record.subscription.*, 'AMER' as region
from bolt_dcp_prod.bronze.raw_s2s_subscription_entities

UNION all

select tenant, record.subscription.*, 'LATAM' as region
from bolt_dcp_prod.beam_latam_bronze.raw_s2s_subscription_entities

UNION all 

select tenant, record.subscription.*, 'EMEA' as region
from bolt_dcp_prod.beam_emea_bronze.raw_s2s_subscription_entities

)

select
s.tenant,
s.realm,
s.region,
s.origin,
replace(s.status,'STATUS_','') as status,
s.startedWithFreeTrial,
s.inFreeTrial,
s.subscribedInSite,
s.purchaseTerritory,

case
  when s.direct.paymentProvider is not null then 'Direct'
  when s.iap.provider is not null then 'IAP'
  when s.partner.partnerId is not null then 'Partner'
end as provider_type,

coalesce(p.provider,'WEB') as provider_name,
direct.subscriptionType as direct_subscriptionType,
s.direct.affiliate as affiliate,

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
coalesce(direct.campaignCode,iap.campaignCode) as campaign_code,
coalesce(direct.campaignId,iap.campaignId) as campaign_id,
p.campaignName,
p.internalname,
replace(p.priceplantype,'TYPE_','') as priceplantype,
replace(p.paymentPeriod,'PERIOD_','') as paymentperiod,
p.numberOfPeriodsBetweenPayments,
p.plan_price as plan_price,
p.currency as plan_currency,

s.startDate,
s.direct.affiliateStartDate as affiliateStartDate,
s.nextRenewalDate,
s.nextRetryDate,
s.endDate,
s.cancellationDate,
s.terminationDate,
s.terminationReason,
s.terminationCode,
s.direct.pricePlanChangeDate as pricePlanChangeDate,
s.direct.minimunTermEndDate as minimumTermEndDate,
s.direct.subscriptionInstallment.renewalCycleStartDate as Installment_renewalCycleStartDate,
s.direct.subscriptionInstallment.remainingInstalments as remainingInstallments,
coalesce(s.direct.verifiedHomeTerritory.expirationDate,s.iap.verifiedHomeTerritory.expirationDate) as HomeTerritory_expirationDate,
coalesce(s.direct.verifiedHomeTerritory.code,s.iap.verifiedHomeTerritory.code) as HomeTerritory_code,

p.contractId,
p.fulfillerPartnerId,
p.fulfillerPartnerSku,
s.globalsubscriptionid,
s.baseSubscriptionId,
s.previousSubscriptionGlobalId,
s.userid,
s.direct.paymentMethodId as paymentMethodId,

s.iap.originalProviderPaymentReference as iap_originalProviderPaymentReference,
s.iap.providerUserId as iap_providerUserId,
s.iap.provider as iap_provider,
s.iap.iapSubscriptionId as iap_iapSubscriptionId,
s.iap.pauseDate as iap_pauseDate,
s.iap.pauseCode as iap_pauseCode,
s.iap.pauseReason as iap_pauseReason,

s.partner.gauthSubscriptionId as partner_gauthSubscriptionId,
s.partner.gauthUserId as partner_gauthUserId,
s.partner.partnerId as partner_partnerId,
s.partner.sku as partner_sku,

s.created as record_created,
s.updated as record_updated,

u.email as user_email,
u.anonymous,
u.testUSer,
u.registeredTimestamp as user_registered_date,
u.lastLoginTimestamp as User_lastlogin_date

from global_subscription_entities s 
left join bolt_finint_int.silver.fs_max_plans p on s.priceplanid = p.priceplanid
left join bolt_finint_int.silver.fs_max_users u on s.userid = u.userid


-- COMMAND ----------

-- DBTITLE 1,Transactions
CREATE or REPLACE MATERIALIZED VIEW bolt_finint_int.silver.fs_max_transactions as

with global_payment_entities as (

select record.transaction.*, 'AMER' as region
from bolt_dcp_prod.bronze.raw_s2s_payment_transaction_entities

UNION all

select record.transaction.*, 'LATAM' as region
from bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_transaction_entities

UNION all 

select record.transaction.*, 'EMEA' as region
from bolt_dcp_prod.any_emea_bronze.raw_s2s_payment_transaction_entities

)
,

exploded_entities as (

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
region,
userId,
created as created_date,
st.event.occurred as event_occurred_date,
source.type as source_type,
source.reference as source_reference,

--populates the correct amount value depending on the event type
case
  when st.event.refunding.source is not null then 
    coalesce(
            st.event.refunding.amountDetails.amountWithTax.minorAmountInclTax * -1, 
            amountDetails.amountWithTax.minorAmountInclTax *-1,
            amountDetails.simpleAmount.minorAmount *-1
            )
  when st.event.refunded.providerRefundReference is not null then 
      coalesce(
            st.event.refunded.amountDetails.amountWithTax.minorAmountInclTax * -1, 
            amountDetails.amountWithTax.minorAmountInclTax *-1,
            amountDetails.simpleAmount.minorAmount *-1
            )
  when st.event.chargeback.providerChargebackReference is not null then 
      coalesce(
            st.event.chargeback.amountDetails.amountWithTax.minorAmountInclTax * -1, 
            amountDetails.amountWithTax.minorAmountInclTax *-1,
            amountDetails.simpleAmount.minorAmount *-1
            )
  else     coalesce(
            amountDetails.amountWithTax.minorAmountInclTax,
            amountDetails.simpleAmount.minorAmount
            )
end as billed_amount,

case
  when st.event.refunding.source is not null then 
    coalesce(
            st.event.refunding.amountDetails.amountWithTax.taxMinorAmount * -1, 
            amountDetails.amountwithTax.taxMinorAmount *-1
            )
  when st.event.refunded.providerRefundReference is not null then 
    coalesce(
            st.event.refunded.amountDetails.amountWithTax.taxMinorAmount * -1, 
            amountDetails.amountwithTax.taxMinorAmount *-1
            )
  when st.event.chargeback.providerChargebackReference is not null then 
    coalesce(
            st.event.chargeback.amountDetails.amountWithTax.taxMinorAmount * -1, 
            amountDetails.amountwithTax.taxMinorAmount *-1
            )
  else     coalesce(
            amountDetails.amountwithTax.taxMinorAmount,0)
end as Tax_amount,

amountDetails.currencyCode as currencyCode,
amountDetails.amountWithTax.taxRateMinorUnits/10000 as tax_rate,
amountdetails.amountWithTax.components[0].description as tax_description,
-- amountDetails.amountwithTax.taxRateMinorUnits as tax_rate_minor_units,
UPPER(amountDetails.amountwithTax.taxationCountryCode) as taxation_country_code,
-- amountDetails.amountwithTax.taxDocumentReference as tax_document_reference,

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

from global_payment_entities
LATERAL VIEW explode_outer(stateTransitions) st as event -- this explodes out every state in the StateTranstions struct - should mirror events
)


select

---MAIN PAYMENT ENTITIY FIELDS---
t.realm,
t.region,
t.userId,
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

t.next_retry_date,
t.source_type,
t.currencyCode as billed_currency,

t.event_type,
t.event_state_reason,
t.refund_source,
t.payment_provider,
t.payment_type,



---PAYMENT METHOD ENTITIY INFO---
pm.paymentMethod_type as payment_method_type,
pm.cardDetails_cardProvider as card_Provider,
pm.cardDetails_fundingSource as funding_source,

---PLANS PRODUCTs CAMPAGINS ENTITIY INFO---
replace(p.market,'TG_MARKET_','')as priceplan_market,
p.launch_wave,
replace(p.producttype,'PRODUCT_TYPE_','') as product_type,
p.bundle as is_bundle,
p.retention_offer as is_retention_offer,
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

---SUBSCRIPTION ENTITY INFO---
s.origin as sub_origin,
s.purchaseTerritory as sub_purchase_territory,
s.affiliate as sub_affiliate,
startDate as sub_start_date,
affiliateStartDate as sub_affiliate_start_date,

---AMOUNTS & CHARGE INFO---
round(coalesce(t.billed_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as billed_amount,
round(coalesce(t.tax_amount, 0) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as tax_amount,
round((coalesce(t.billed_amount, 0) - coalesce(t.tax_amount, 0)) / POWER(10, coalesce(p.currencyDecimalPoints,0)),2) as revenue_amount,
t.tax_rate,
t.tax_description,
UPPER(t.taxation_country_code) as tax_country_code

---ALL REFERENCE IDs---
t.userid,
t.event_id,
t.original_transaction_id,
t.provider_Reference_id,
t.merchantaccount,
t.paymentmethodid,
s.globalsubscriptionid,
s.basesubscriptionid,
s.previoussubscriptionglobalid,
s.priceplanid,

---OTHER USEFUL ADHOCINFO
s.testUSer

from exploded_entities t
left join bolt_finint_int.silver.fs_max_subscriptions s on t.source_reference = s.globalsubscriptionid
left join bolt_finint_int.silver.fs_max_plans p on p.priceplanid = s.priceplanid
left join bolt_finint_int.silver.fs_paymentmethods pm on t.paymentmethodid = pm.paymentMethodId


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Finance Views
