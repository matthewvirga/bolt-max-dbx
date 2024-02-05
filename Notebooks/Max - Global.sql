-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Global Max Reporting
-- MAGIC Combined regional tables to create Global reporting for MAX

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DCP Bronze Raw
-- MAGIC source catalog: bolt_dcp_prod

-- COMMAND ----------

-- DBTITLE 1,Plans
create
or replace temporary view Plans AS
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
group by
  all

-- COMMAND ----------

-- DBTITLE 1,Transactions
with combined_tx as 
(
  select * from bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_transaction_entities
  union
  select * from bolt_dcp_prod.bronze.raw_s2s_payment_transaction_entities
)





-- COMMAND ----------

-- DBTITLE 1,Subscriptions
create
or replace temporary view Subscriptions AS
SELECT
  *
FROM
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
      iap.iapSubscriptionId AS iap_SubscriptionId,
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
    from
      bolt_finint_prod.silver.fi_subscriptionv2_enriched
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
    from
      bolt_finint_prod.latam_silver.fi_subscriptionv2_enriched
  )

-- COMMAND ----------

-- DBTITLE 1,Transactions
create
or replace temporary view Transactions AS
select
  *
from
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
    from
      bolt_finint_prod.silver.fi_transactionv2_enriched
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
    from
      bolt_finint_prod.latam_silver.fi_transactionv2_enriched
  )

-- COMMAND ----------

-- DBTITLE 1,PaymentMethod
create
or replace temporary view PaymentMethod AS
select
  *
from
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
    from
      bolt_finint_prod.silver.fi_paymentmethodv2_enriched
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
    from
      bolt_finint_prod.latam_silver.fi_paymentmethodv2_enriched
  )

-- COMMAND ----------

-- DBTITLE 1,Users
create
or replace temporary view Users AS
select
  *
from
  (
    select *      
    from bolt_finint_prod.silver.fi_user_enriched
    UNION ALL
    select *
    from bolt_finint_prod.latam_silver.fi_user_enriched
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Global Reports
-- MAGIC This creates global combined reports

-- COMMAND ----------

-- DBTITLE 1,Active Subscribers
select

-- Geo Location Info --

  UPPER(s.realm) as realm,
  pl.region,
  pl.market,
  UPPER(s.subscribedInTerritory) as subscribedInTerritory,
  UPPER(s.purchaseTerritory) as purchaseTerritory,
  pm.geoLocation_countryIso as PM_Geo_Country,

-- Subscription Info --

  s.direct_subscriptionType as subscription_type,
  s.status,
  case
    when s.direct_paymentProvider is not null then 'Direct'
    when s.iap_provider is not null then 'IAP'
    when s.partner_gauthUserId is not null then 'Partner'
    else 'Unknown'
  end as provider_type,
  case
    when s.direct_paymentProvider is not null then 'WEB'
    when s.iap_provider is not null then UPPER(s.iap_provider)
    else 'Unknown'
  end as provider,
  s.direct_affiliate,
  s.direct_subscriptionInstallment_remainingInstalments as Installments_Remaining,
  s.iap_pauseCode,
  s.iap_pauseReason,
  s.terminationCode,
  s.terminationReason,
  s.startedWithFreeTrial,
  s.inFreeTrial,
  s.subscribedInSite,

-- Plans Info --

  s.priceplanid,
  pl.tiertype,
  pl.producttype,
  pl.productname,
  pl.addOnIds,
  pl.mainProductIds,
  pl.campaignName,
  pl.internalname,
  pl.priceplantype,
  pl.paymentPeriod,
  pl.numberOfInstalments,
  pl.numberOfPeriodsBetweenPayments,
  -- pl.price,
  -- pl.currencyDecimalPoints,
  pl.ConvertedPrice,
  pl.currency,

-- Key Date Fields --

  s.direct_affiliateStartDate::date,
  s.startDate::date,
  s.direct_pricePlanChangeDate::date,
  s.nextRenewalDate::date,
  s.nextRetryDate::date,
  s.iap_pauseDate::date,
  s.direct_subscriptionInstallment_renewalCycleStartDate::date as Installment_Renewal_Cycle_Date,
  s.direct_minimumTermEndDate::date,
  s.endDate::date,
  s.cancellationDate::date,
  s.terminationDate::date,

-- Payment / transactional Info --

  case
    when s.direct_paymentProvider is not null then Upper(s.direct_paymentProvider)
    when s.iap_provider is not null then UPPER(s.iap_provider)
    else 'Unknown'
  end as Payment_Provider,
  UPPER(pm.provider) as PM_provider,
  pm.countryCode as PM_countryCode,
  pm.cardDetails_cardProvider as cardProvider,
  pm.cardDetails_cardPaymentMethod as cardPaymentMethod,
  pm.cardDetails_cardIssuingBank as cardIssuingBank,
  pm.cardDetails_fundingSource as fundingSource,
  pm.cardDetails_cardSummary as cardSummary,
  pm.providerMerchantAccount,

-- Ids --

  -- Direct --
  s.globalSubscriptionId,
  s.previousSubscriptionGlobalId,
  s.userId,
  s.pricePlanId,
  s.direct_paymentMethodId,
  pm.providerAgreementReference,
  pm.providerUserReference,
  pm.legacyPaymentMethodId,

  -- IAP --
  s.iap_providerUserId,
  s.iap_SubscriptionId,
  s.iap_originalProviderPaymentReference,

  -- Partner --
  s.partner_gauthUserId,
  s.partner_gauthSubscriptionId,
  s.partner_partnerId

from Subscriptions S

LEFT JOIN Plans Pl ON s.priceplanid = pl.priceplanid
LEFT JOIN PaymentMethod Pm ON pm.legacyPaymentMethodId = s.direct_paymentmethodid
LEFT JOIN Users U ON 

-- COMMAND ----------

-- DBTITLE 1,Transactions Extract
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

