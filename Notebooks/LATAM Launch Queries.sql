-- Databricks notebook source
-- DBTITLE 1,Latam Plans
create or replace temporary view Plans as
(
select
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
  -- coalesce(pp.price, 0) as price,
  pp.currencyDecimalPoints,
  CASE
    WHEN pp.currencyDecimalPoints IS NOT NULL THEN pp.price / POWER(10, pp.currencyDecimalPoints)
    ELSE coalesce(pp.price, 0)
  END AS Price,
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
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.amountWithTax.minorAmountInclTax * -1
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.amountWithTax.minorAmountInclTax * -1
  else amountDetails.amountWithTax.minorAmountInclTax
end as charged_amount,

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.currencyCode
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.currencyCode
  else amountDetails.currencyCode
end as currency,

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.amountWithTax.taxMinorAmount * -1
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.amountWithTax.taxMinorAmount * -1
  else amountDetails.amountWithTax.taxMinorAmount
end as tax_amount,

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.amountWithTax.components[0].description
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.amountWithTax.components[0].description
  else amountDetails.amountWithTax.components[0].description
end as tax_description,

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.amountWithTax.taxRateMinorUnits/10000
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.amountWithTax.taxRateMinorUnits/10000
  else amountDetails.amountWithTax.taxRateMinorUnits/10000
end as tax_rate,

case
  when st.event.refunded.providerRefundReference is not null then st.event.refunded.amountDetails.amountWithTax.taxationCountryCode
  when st.event.chargeback.providerChargebackReference is not null then st.event.chargeback.amountDetails.amountWithTax.taxationCountryCode
  else amountDetails.amountWithTax.taxationCountryCode
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
  when st.event.refunding.providerRefundReference is not null then 'REFUNDING'
  when st.event.refunded.providerRefundReference is not null then 'REFUNDED'
  when st.event.refundFailed.reason is not null then 'REFUND FAILED'
  when st.event.chargeback.providerChargebackReference is not null then 'CHARGEBACK'
  when st.event.timedOut.emptyObject is not null then 'TIMED OUT'
  when st.event.revoked.reason is not null then 'REVOKED'
  when st.event.chargebackRejected.providerChargebackReference is not null then 'CHARGEBACK REJECTED'
  when (st.event.corrected.providerPaymentReference is not null OR st.event.corrected.providerRefundReference is not null) then 'CORRECTED'
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
merchantAccount,
items[0].subscriptionDetails.serviceperiod.startdate as service_period_startdate,
items[0].subscriptionDetails.serviceperiod.enddate as service_period_enddate,
st.event.retrying.nextretry as next_retry_date

from bolt_finint_prod.latam_silver.fi_transactionv2_enriched
LATERAL VIEW explode(stateTransitions) st as event
group by all

)
