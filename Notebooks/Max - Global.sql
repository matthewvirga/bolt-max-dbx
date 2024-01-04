-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Global Tables

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

create or replace temporary view Global_Transactions AS
