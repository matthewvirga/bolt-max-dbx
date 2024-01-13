-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Global Views Creation
-- MAGIC This creates a global view for all columns in each region's table

-- COMMAND ----------

-- DBTITLE 1,Plans
create or replace temporary view Plans AS

select
case
 when marketid = 'TG_MARKET_UNITED_STATES' then 'AMER' else 'LATAM'
end as Region,
marketId as market,
pp.id as priceplanid,
pr.addOnIds[0] as addOnIds,
pr.mainProductIds[0] as mainProductIds,
pr.productType as producttype,
pr.name as productname,
pr.tiertype as tiertype,
if(pp.provider='WEB', 'Direct', provider) as Provider,
ca.name as campaignName,
pp.internalName as internalname,
pp.pricePlanType as priceplantype,
pp.period as paymentPeriod,
pp.numberOfInstalments,
pp.numberOfPeriodsBetweenPayments,
coalesce(pp.price,0) as price,
pp.currencyDecimalPoints,
CASE 
 WHEN pp.currencyDecimalPoints IS NOT NULL 
 THEN pp.price / POWER(10, pp.currencyDecimalPoints)
 ELSE coalesce(pp.price,0)
END AS ConvertedPrice,
pp.currency as currency
from bolt_finint_prod.silver.fi_priceplanv2_enriched pp
LEFT JOIN bolt_finint_prod.silver.fi_product_enriched pr on pp.productid=pr.id
LEFT JOIN bolt_finint_prod.silver.fi_campaignv2_enriched ca on pp.id = ca.pricePlanId
group by all

-- COMMAND ----------

-- DBTITLE 1,Subscriptions
create or replace temporary view Subscriptions AS

SELECT *
FROM (
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

        direct.verifiedHomeTerritory.expirationDate AS direct_verifiedHomeTerritory_expirationDate,
        direct.verifiedHomeTerritory.code AS direct_verifiedHomeTerritory_code,
        direct.subscriptionInstallment.remainingInstalments AS direct_subscriptionInstallment_remainingInstalments,
        direct.subscriptionInstallment.renewalCycleStartDate AS direct_subscriptionInstallment_renewalCycleStartDate,
        direct.campaignCode AS direct_campaignCode,
        direct.campaignId AS direct_campaignId,
        direct.minimumTermEndDate AS direct_minimumTermEndDate,
        direct.affiliate AS direct_affiliate,
        direct.affiliateStartDate AS direct_affiliateStartDate,
        direct.paymentProvider AS direct_paymentProvider,
        direct.subscriptionType AS direct_subscriptionType,
        direct.terminationAllowed AS direct_terminationAllowed,
        direct.pricePlanChangeDate AS direct_pricePlanChangeDate,
        direct.paymentMethodId AS direct_paymentMethodId,

        iap.verifiedHomeTerritory.expirationDate AS iap_verifiedHomeTerritory_expirationDate,
        iap.verifiedHomeTerritory.code AS iap_verifiedHomeTerritory_code,
        iap.campaignCode AS iap_campaignCode,
        iap.campaignId AS iap_campaignId,
        iap.originalProviderPaymentReference AS iap_originalProviderPaymentReference,
        iap.providerUserId AS iap_providerUserId,
        iap.provider AS iap_provider,
        iap.iapSubscriptionId AS iap_iapSubscriptionId,
        iap.pauseDate AS iap_pauseDate,
        iap.pauseCode AS iap_pauseCode,
        iap.pauseReason AS iap_pauseReason,

        partner.gauthUserId AS partner_gauthUserId,
        partner.gauthSubscriptionId AS partner_gauthSubscriptionId,
        partner.partnerId AS partner_partnerId,
        partner.sku AS partner_sku,

        terminationDate,
        terminationReason,
        terminationCode,
        startedWithFreeTrial,
        nextRetryDate,
        origin,
        subscribedInTerritory,
        previousSubscriptionGlobalId,
        paymentMethodId,
        inFreeTrial,
        subscribedInSite,
        purchaseTerritory
    from bolt_finint_prod.silver.fi_subscriptionv2_enriched

    UNION all

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

        direct.verifiedHomeTerritory.expirationDate AS direct_verifiedHomeTerritory_expirationDate,
        direct.verifiedHomeTerritory.code AS direct_verifiedHomeTerritory_code,
        direct.subscriptionInstallment.remainingInstalments AS direct_subscriptionInstallment_remainingInstalments,
        direct.subscriptionInstallment.renewalCycleStartDate AS direct_subscriptionInstallment_renewalCycleStartDate,
        direct.campaignCode AS direct_campaignCode,
        direct.campaignId AS direct_campaignId,
        direct.minimumTermEndDate AS direct_minimumTermEndDate,
        direct.affiliate AS direct_affiliate,
        direct.affiliateStartDate AS direct_affiliateStartDate,
        direct.paymentProvider AS direct_paymentProvider,
        direct.subscriptionType AS direct_subscriptionType,
        direct.terminationAllowed AS direct_terminationAllowed,
        direct.pricePlanChangeDate AS direct_pricePlanChangeDate,
        direct.paymentMethodId AS direct_paymentMethodId,

        iap.verifiedHomeTerritory.expirationDate AS iap_verifiedHomeTerritory_expirationDate,
        iap.verifiedHomeTerritory.code AS iap_verifiedHomeTerritory_code,
        iap.campaignCode AS iap_campaignCode,
        iap.campaignId AS iap_campaignId,
        iap.originalProviderPaymentReference AS iap_originalProviderPaymentReference,
        iap.providerUserId AS iap_providerUserId,
        iap.provider AS iap_provider,
        iap.iapSubscriptionId AS iap_iapSubscriptionId,
        iap.pauseDate AS iap_pauseDate,
        iap.pauseCode AS iap_pauseCode,
        iap.pauseReason AS iap_pauseReason,

        partner.gauthUserId AS partner_gauthUserId,
        partner.gauthSubscriptionId AS partner_gauthSubscriptionId,
        partner.partnerId AS partner_partnerId,
        partner.sku AS partner_sku,

        terminationDate,
        terminationReason,
        terminationCode,
        startedWithFreeTrial,
        nextRetryDate,
        origin,
        subscribedInTerritory,
        previousSubscriptionGlobalId,
        paymentMethodId,
        inFreeTrial,
        subscribedInSite,
        purchaseTerritory
    from bolt_finint_prod.latam_silver.fi_subscriptionv2_enriched
)

-- COMMAND ----------

-- DBTITLE 1,Transactions
create or replace temporary view Transactions AS
select * from 

(

select
id,
realm,
userId,
created,

source.type as source_type,
source.reference as source_reference,

amountDetails.simpleAmount.minorAmount as amountDetails_simpleAmount_minorAmount,
amountDetails.simpleAmount.taxationCountryCode as amountDetails_simpleAmount_taxationCountryCode,
amountDetails.amountWithTax.minorAmountInclTax as amountDetails_amountWithTax_minorAmountInclTax,
amountDetails.amountWithTax.taxMinorAmount as amountDetails_amountWithTax_taxMinorAmount,
amountDetails.amountWithTax.taxRateMinorUnits as amountDetails_amountWithTax_taxRateMinorUnits,
amountDetails.amountWithTax.taxationCountryCode as amountDetails_amountWithTax_taxationCountryCode,
amountDetails.amountWithTax.taxDocumentReference as amountDetails_amountWithTax_taxDocumentReference,
amountDetails.amountWithTax.taxRate.scale as amountDetails_amountWithTax_taxRate_scale,
amountDetails.amountWithTax.taxRate.precision as amountDetails_amountWithTax_taxRate_precision,
amountDetails.amountWithTax.taxRate.value as amountDetails_amountWithTax_taxRate_value,
amountDetails.currencyCode as amountDetails_currencyCode,

paymentMethodId,

currentState.occurred as currentState_occurred,
currentState.pending.providerPaymentReference as currentState_pending_providerPaymentReference,
currentState.successful.providerPaymentReference as currentState_successful_providerPaymentReference,
currentState.canceled.reason as currentState_canceled_reason,
currentState.failed.reason as currentState_failed_reason,
currentState.failed.providerPaymentReference as currentState_failed_providerPaymentReference,
currentState.retrying.nextRetry as currentState_retrying_nextRetry,
currentState.retrying.reason as currentState_retrying_reason,
currentState.refunding.amountDetails.simpleAmount.minorAmount as currentState_refunding_simpleAmount_simpleAmount_minorAmount,
currentState.refunding.amountDetails.simpleAmount.taxationCountryCode as currentState_refunding_amountDetails_simpleAmount_taxationCountryCode,
currentState.refunding.amountDetails.amountWithTax.minorAmountInclTax as currentState_refunding_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refunding.amountDetails.amountWithTax.taxMinorAmount as currentState_refunding_amountDetails_amountWithTax_taxMinorAmount,
currentState.refunding.amountDetails.amountWithTax.taxRateMinorUnits as currentState_refunding_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refunding.amountDetails.amountWithTax.taxationCountryCode as currentState_refunding_amountDetails_amountWithTax_taxationCountryCode,
currentState.refunding.amountDetails.amountWithTax.taxDocumentReference as currentState_refunding_amountDetails_amountWithTax_taxDocumentReference,
currentState.refunding.amountDetails.amountWithTax.taxRate.scale as currentState_refunding_amountDetails_amountWithTax_taxRate_scale,
currentState.refunding.amountDetails.amountWithTax.taxRate.precision as currentState_refunding_amountDetails_amountWithTax_taxRate_precision,
currentState.refunding.amountDetails.amountWithTax.taxRate.value as currentState_refunding_amountDetails_amountWithTax_taxRate_value,
currentState.refunding.amountDetails.currencyCode as currentState_refunding_amountDetails_currencyCode,
currentState.refunding.providerRefundReference as currentState_refunding_providerRefundReference,

currentState.refunded.amountDetails.simpleAmount.minorAmount as currentState_refunded_simpleAmount_simpleAmount_minorAmount,
currentState.refunded.amountDetails.simpleAmount.taxationCountryCode as currentState_refunded_amountDetails_simpleAmount_taxationCountryCode,
currentState.refunded.amountDetails.amountWithTax.minorAmountInclTax as currentState_refunded_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refunded.amountDetails.amountWithTax.taxMinorAmount as currentState_refunded_amountDetails_amountWithTax_taxMinorAmount,
currentState.refunded.amountDetails.amountWithTax.taxRateMinorUnits as currentState_refunded_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refunded.amountDetails.amountWithTax.taxationCountryCode as currentState_refunded_amountDetails_amountWithTax_taxationCountryCode,
currentState.refunded.amountDetails.amountWithTax.taxDocumentReference as currentState_refunded_amountDetails_amountWithTax_taxDocumentReference,
currentState.refunded.amountDetails.amountWithTax.taxRate.scale as currentState_refunded_amountDetails_amountWithTax_taxRate_scale,
currentState.refunded.amountDetails.amountWithTax.taxRate.precision as currentState_refunded_amountDetails_amountWithTax_taxRate_precision,
currentState.refunded.amountDetails.amountWithTax.taxRate.value as currentState_refunded_amountDetails_amountWithTax_taxRate_value,
currentState.refunded.amountDetails.currencyCode as currentState_refunded_amountDetails_currencyCode,
currentState.refunded.refundReference as currentState_refunded_refundReference,
currentState.refunded.providerRefundReference as currentState_refunded_providerRefundReference,
currentState.refunded.source as currentState_refunded_source,

currentState.refundFailed.amountDetails.simpleAmount.minorAmount as currentState_refundFailed_simpleAmount_simpleAmount_minorAmount,
currentState.refundFailed.amountDetails.simpleAmount.taxationCountryCode as currentState_refundFailed_amountDetails_simpleAmount_taxationCountryCode,
currentState.refundFailed.amountDetails.amountWithTax.minorAmountInclTax as currentState_refundFailed_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refundFailed.amountDetails.amountWithTax.taxMinorAmount as currentState_refundFailed_amountDetails_amountWithTax_taxMinorAmount,
currentState.refundFailed.amountDetails.amountWithTax.taxRateMinorUnits as ccurrentState_refundFailed_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refundFailed.amountDetails.amountWithTax.taxationCountryCode as currentState_refundFailed_amountDetails_amountWithTax_taxationCountryCode,
currentState.refundFailed.amountDetails.amountWithTax.taxDocumentReference as currentState_refundFailed_amountDetails_amountWithTax_taxDocumentReference,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.scale as currentState_refundFailed_amountDetails_amountWithTax_taxRate_scale,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.precision as currentState_refundFailed_amountDetails_amountWithTax_taxRate_precision,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.value as currentState_refundFailed_amountDetails_amountWithTax_taxRate_value,
currentState.refundFailed.reason as currentState_refundFailed_reason,

currentState.chargeback.amountDetails.simpleAmount.minorAmount as currentState_chargeback_simpleAmount_simpleAmount_minorAmount,
currentState.chargeback.amountDetails.simpleAmount.taxationCountryCode as currentState_chargeback_amountDetails_simpleAmount_taxationCountryCode,
currentState.chargeback.amountDetails.amountWithTax.minorAmountInclTax as currentState_chargeback_amountDetails_amountWithTax_minorAmountInclTax,
currentState.chargeback.amountDetails.amountWithTax.taxMinorAmount as currentState_chargeback_amountDetails_amountWithTax_taxMinorAmount,
currentState.chargeback.amountDetails.amountWithTax.taxRateMinorUnits as currentState_chargeback_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.chargeback.amountDetails.amountWithTax.taxationCountryCode as currentState_chargeback_amountDetails_amountWithTax_taxationCountryCode,
currentState.chargeback.amountDetails.amountWithTax.taxDocumentReference as currentState_chargeback_amountDetails_amountWithTax_taxDocumentReference,
currentState.chargeback.amountDetails.amountWithTax.taxRate.scale as currentState_chargeback_amountDetails_amountWithTax_taxRate_scale,
currentState.chargeback.amountDetails.amountWithTax.taxRate.precision as currentState_chargeback_amountDetails_amountWithTax_taxRate_precision,
currentState.chargeback.amountDetails.amountWithTax.taxRate.value as currentState_chargeback_amountDetails_amountWithTax_taxRate_value,
currentState.chargeback.amountDetails.currencyCode as currentState_chargeback_amountDetails_currencyCode,
currentState.chargeback.providerChargebackReference as currentState_chargeback_providerChargebackReference,

currentState.timedOut.emptyObject as currentState_timedOut_emptyObject,

currentState.revoked.reason as currentState_revoked_reason,

currentState.chargebackRejected.reason as currentState_chargebackRejected_reason,
currentState.chargebackRejected.providerChargebackReference as currentState_chargebackRejected_providerChargebackReference,

currentState.corrected.providerPaymentReference as currentState_corrected_providerPaymentReference,
currentState.corrected.providerRefundReference as currentState_corrected_providerRefundReference,

provider,
billingAddressId,
type,
merchantAccount

from bolt_finint_prod.silver.fi_transactionv2_enriched

UNION ALL

select
id,
realm,
userId,
created,

source.type as source_type,
source.reference as source_reference,

amountDetails.simpleAmount.minorAmount as amountDetails_simpleAmount_minorAmount,
amountDetails.simpleAmount.taxationCountryCode as amountDetails_simpleAmount_taxationCountryCode,
amountDetails.amountWithTax.minorAmountInclTax as amountDetails_amountWithTax_minorAmountInclTax,
amountDetails.amountWithTax.taxMinorAmount as amountDetails_amountWithTax_taxMinorAmount,
amountDetails.amountWithTax.taxRateMinorUnits as amountDetails_amountWithTax_taxRateMinorUnits,
amountDetails.amountWithTax.taxationCountryCode as amountDetails_amountWithTax_taxationCountryCode,
amountDetails.amountWithTax.taxDocumentReference as amountDetails_amountWithTax_taxDocumentReference,
amountDetails.amountWithTax.taxRate.scale as amountDetails_amountWithTax_taxRate_scale,
amountDetails.amountWithTax.taxRate.precision as amountDetails_amountWithTax_taxRate_precision,
amountDetails.amountWithTax.taxRate.value as amountDetails_amountWithTax_taxRate_value,
amountDetails.currencyCode as amountDetails_currencyCode,

paymentMethodId,

currentState.occurred as currentState_occurred,
currentState.pending.providerPaymentReference as currentState_pending_providerPaymentReference,
currentState.successful.providerPaymentReference as currentState_successful_providerPaymentReference,
currentState.canceled.reason as currentState_canceled_reason,
currentState.failed.reason as currentState_failed_reason,
currentState.failed.providerPaymentReference as currentState_failed_providerPaymentReference,
currentState.retrying.nextRetry as currentState_retrying_nextRetry,
currentState.retrying.reason as currentState_retrying_reason,
currentState.refunding.amountDetails.simpleAmount.minorAmount as currentState_refunding_simpleAmount_simpleAmount_minorAmount,
currentState.refunding.amountDetails.simpleAmount.taxationCountryCode as currentState_refunding_amountDetails_simpleAmount_taxationCountryCode,
currentState.refunding.amountDetails.amountWithTax.minorAmountInclTax as currentState_refunding_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refunding.amountDetails.amountWithTax.taxMinorAmount as currentState_refunding_amountDetails_amountWithTax_taxMinorAmount,
currentState.refunding.amountDetails.amountWithTax.taxRateMinorUnits as currentState_refunding_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refunding.amountDetails.amountWithTax.taxationCountryCode as currentState_refunding_amountDetails_amountWithTax_taxationCountryCode,
currentState.refunding.amountDetails.amountWithTax.taxDocumentReference as currentState_refunding_amountDetails_amountWithTax_taxDocumentReference,
currentState.refunding.amountDetails.amountWithTax.taxRate.scale as currentState_refunding_amountDetails_amountWithTax_taxRate_scale,
currentState.refunding.amountDetails.amountWithTax.taxRate.precision as currentState_refunding_amountDetails_amountWithTax_taxRate_precision,
currentState.refunding.amountDetails.amountWithTax.taxRate.value as currentState_refunding_amountDetails_amountWithTax_taxRate_value,
currentState.refunding.amountDetails.currencyCode as currentState_refunding_amountDetails_currencyCode,
currentState.refunding.providerRefundReference as currentState_refunding_providerRefundReference,

currentState.refunded.amountDetails.simpleAmount.minorAmount as currentState_refunded_simpleAmount_simpleAmount_minorAmount,
currentState.refunded.amountDetails.simpleAmount.taxationCountryCode as currentState_refunded_amountDetails_simpleAmount_taxationCountryCode,
currentState.refunded.amountDetails.amountWithTax.minorAmountInclTax as currentState_refunded_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refunded.amountDetails.amountWithTax.taxMinorAmount as currentState_refunded_amountDetails_amountWithTax_taxMinorAmount,
currentState.refunded.amountDetails.amountWithTax.taxRateMinorUnits as currentState_refunded_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refunded.amountDetails.amountWithTax.taxationCountryCode as currentState_refunded_amountDetails_amountWithTax_taxationCountryCode,
currentState.refunded.amountDetails.amountWithTax.taxDocumentReference as currentState_refunded_amountDetails_amountWithTax_taxDocumentReference,
currentState.refunded.amountDetails.amountWithTax.taxRate.scale as currentState_refunded_amountDetails_amountWithTax_taxRate_scale,
currentState.refunded.amountDetails.amountWithTax.taxRate.precision as currentState_refunded_amountDetails_amountWithTax_taxRate_precision,
currentState.refunded.amountDetails.amountWithTax.taxRate.value as currentState_refunded_amountDetails_amountWithTax_taxRate_value,
currentState.refunded.amountDetails.currencyCode as currentState_refunded_amountDetails_currencyCode,
currentState.refunded.refundReference as currentState_refunded_refundReference,
currentState.refunded.providerRefundReference as currentState_refunded_providerRefundReference,
currentState.refunded.source as currentState_refunded_source,

currentState.refundFailed.amountDetails.simpleAmount.minorAmount as currentState_refundFailed_simpleAmount_simpleAmount_minorAmount,
currentState.refundFailed.amountDetails.simpleAmount.taxationCountryCode as currentState_refundFailed_amountDetails_simpleAmount_taxationCountryCode,
currentState.refundFailed.amountDetails.amountWithTax.minorAmountInclTax as currentState_refundFailed_amountDetails_amountWithTax_minorAmountInclTax,
currentState.refundFailed.amountDetails.amountWithTax.taxMinorAmount as currentState_refundFailed_amountDetails_amountWithTax_taxMinorAmount,
currentState.refundFailed.amountDetails.amountWithTax.taxRateMinorUnits as ccurrentState_refundFailed_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.refundFailed.amountDetails.amountWithTax.taxationCountryCode as currentState_refundFailed_amountDetails_amountWithTax_taxationCountryCode,
currentState.refundFailed.amountDetails.amountWithTax.taxDocumentReference as currentState_refundFailed_amountDetails_amountWithTax_taxDocumentReference,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.scale as currentState_refundFailed_amountDetails_amountWithTax_taxRate_scale,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.precision as currentState_refundFailed_amountDetails_amountWithTax_taxRate_precision,
currentState.refundFailed.amountDetails.amountWithTax.taxRate.value as currentState_refundFailed_amountDetails_amountWithTax_taxRate_value,
currentState.refundFailed.reason as currentState_refundFailed_reason,

currentState.chargeback.amountDetails.simpleAmount.minorAmount as currentState_chargeback_simpleAmount_simpleAmount_minorAmount,
currentState.chargeback.amountDetails.simpleAmount.taxationCountryCode as currentState_chargeback_amountDetails_simpleAmount_taxationCountryCode,
currentState.chargeback.amountDetails.amountWithTax.minorAmountInclTax as currentState_chargeback_amountDetails_amountWithTax_minorAmountInclTax,
currentState.chargeback.amountDetails.amountWithTax.taxMinorAmount as currentState_chargeback_amountDetails_amountWithTax_taxMinorAmount,
currentState.chargeback.amountDetails.amountWithTax.taxRateMinorUnits as currentState_chargeback_amountDetails_amountWithTax_taxRateMinorUnits,
currentState.chargeback.amountDetails.amountWithTax.taxationCountryCode as currentState_chargeback_amountDetails_amountWithTax_taxationCountryCode,
currentState.chargeback.amountDetails.amountWithTax.taxDocumentReference as currentState_chargeback_amountDetails_amountWithTax_taxDocumentReference,
currentState.chargeback.amountDetails.amountWithTax.taxRate.scale as currentState_chargeback_amountDetails_amountWithTax_taxRate_scale,
currentState.chargeback.amountDetails.amountWithTax.taxRate.precision as currentState_chargeback_amountDetails_amountWithTax_taxRate_precision,
currentState.chargeback.amountDetails.amountWithTax.taxRate.value as currentState_chargeback_amountDetails_amountWithTax_taxRate_value,
currentState.chargeback.amountDetails.currencyCode as currentState_chargeback_amountDetails_currencyCode,
currentState.chargeback.providerChargebackReference as currentState_chargeback_providerChargebackReference,

currentState.timedOut.emptyObject as currentState_timedOut_emptyObject,

currentState.revoked.reason as currentState_revoked_reason,

currentState.chargebackRejected.reason as currentState_chargebackRejected_reason,
currentState.chargebackRejected.providerChargebackReference as currentState_chargebackRejected_providerChargebackReference,

currentState.corrected.providerPaymentReference as currentState_corrected_providerPaymentReference,
currentState.corrected.providerRefundReference as currentState_corrected_providerRefundReference,

provider,
billingAddressId,
type,
merchantAccount

from bolt_finint_prod.latam_silver.fi_transactionv2_enriched

)



-- COMMAND ----------

-- DBTITLE 1,PaymentMethod
create or replace temporary view PaymentMethod AS

select * from 
(

select

id,
realm,
userId,
created,
provider,
type,
countryCode,
providerAgreementReference,

cardDetails.cardProvider as cardDetails_cardProvider,
cardDetails.cardPaymentMethod as cardDetails_cardPaymentMethod,
cardDetails.cardIssuingBank as cardDetails_cardIssuingBank,
cardDetails.fundingSource as cardDetails_fundingSource,
cardDetails.cardSummary as cardDetails_cardSummary,
cardDetails.cardExpiry as cardDetails_cardExpiry,

legacyPaymentMethodId,
providerMerchantAccount,

geoLocation.countryIso as geoLocation_countryIso,
geoLocation.subdivisionIso as geoLocation_subdivisionIso,
geoLocation.city as geoLocation_city,
geoLocation.postalCode as geoLocation_postalCode,
geoLocation.timeZone as geoLocation_timeZone,
geoLocation.isAnonymized as geoLocation_isAnonymized,

providerUserReference

from bolt_finint_prod.silver.fi_paymentmethodv2_enriched

UNION ALL

select 

id,
realm,
userId,
created,
provider,
type,
countryCode,
providerAgreementReference,

cardDetails.cardProvider as cardDetails_cardProvider,
cardDetails.cardPaymentMethod as cardDetails_cardPaymentMethod,
cardDetails.cardIssuingBank as cardDetails_cardIssuingBank,
cardDetails.fundingSource as cardDetails_fundingSource,
cardDetails.cardSummary as cardDetails_cardSummary,
cardDetails.cardExpiry as cardDetails_cardExpiry,

legacyPaymentMethodId,
providerMerchantAccount,

geoLocation.countryIso as geoLocation_countryIso,
geoLocation.subdivisionIso as geoLocation_subdivisionIso,
geoLocation.city as geoLocation_city,
geoLocation.postalCode as geoLocation_postalCode,
geoLocation.timeZone as geoLocation_timeZone,
geoLocation.isAnonymized as geoLocation_isAnonymized,

providerUserReference

from bolt_finint_prod.latam_silver.fi_paymentmethodv2_enriched

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Global Reports
-- MAGIC This creates global combined reports

-- COMMAND ----------

-- DBTITLE 1,Active Subscribers


-- COMMAND ----------

-- DBTITLE 1,Transactions Extract


-- COMMAND ----------

-- DBTITLE 1,Acquired Subscribers


-- COMMAND ----------

-- DBTITLE 1,Plan Transitions


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Control Reports
-- MAGIC These control reports are necessary for monitoring the billing system's overall health

-- COMMAND ----------

-- DBTITLE 1,Duplicate Transactions


-- COMMAND ----------

-- DBTITLE 1,Duplicate Subscriptions


-- COMMAND ----------

-- DBTITLE 1,Unbilled Subscriptions

