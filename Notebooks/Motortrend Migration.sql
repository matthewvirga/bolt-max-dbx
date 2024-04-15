-- Databricks notebook source
-- DBTITLE 1,Original Motortrend Priceplans
select * from dplus_finint_prod.silver.fi_priceplan_enriched 
where lower(realm) = 'motortrend'
and upper(provider) = 'WEB'


-- COMMAND ----------

-- DBTITLE 1,New Motortrend Priceplans (Dplus)
select * from dplus_finint_prod.silver.fi_priceplan_enriched 
where lower(realm) = 'go'
and lower(internalName) like '%legmt.%' OR lower(internalName) like '%motortrend discovery%'
and upper(provider) = 'WEB'
order by createDate desc


-- COMMAND ----------

-- DBTITLE 1,User Migration Check
with user_mapping as (
  select 
  legacy_user_id,
  sonic_profile_id,
  sonic_user_id
  from bolt_finint_int.bronze.motortrend_user_mapping
  where ignored = false
)

select
go.createDate::date as created_in_dplus,
count(go.id)
from user_mapping um 
left join dplus_finint_prod.silver.fi_user_enriched_kinesis go 
  on um.sonic_user_id = go.id
group by all



-- COMMAND ----------

-- DBTITLE 1,Migrated Motortrend Subscriptions
with new_mt_plans as
(
select * from dplus_finint_prod.silver.fi_priceplan_enriched 
where lower(realm) = 'go'
and lower(internalName) like '%legmt.%' OR lower(internalName) like '%motortrend discovery%'
order by createDate desc
)
,
user_mapping as (
  select 
  legacy_user_id,
  sonic_profile_id,
  sonic_user_id
  from bolt_finint_int.bronze.motortrend_user_mapping
  where ignored = false
)

select 
s.realm,
s.id,
s.globalId,
s.userId,
s.subscriptionType,
s.status,
s.nestedPricePlanId,
s.mainSubscriptionGlobalId,
s.terminationCode,
s.terminationReason,
s.startedWithFreeTrial,
s.inFreeTrial,
s.startDate::date,
s.endDate::date,
s.nextRenewalDate::date,
s.cancellationDate::date,
s.terminationDate::date,
s.subscribedInTerritory,
s.affiliate,
s.pauseDate::date,
s.pauseReason,
s.pauseCode,
pl.id as price_plan_id,
pl.price/100 as plan_price,
pl.period,
pl.pricePlanType,
pl.provider,
pl.internalName,
pl.market,
pl.numberOfPeriods,
pl.category,
pl.providerPartnerId,
pl.providerIntegrationType

from dplus_finint_prod.silver.fi_subscription_enriched s
right join new_mt_plans pl on s.nestedPricePlanId = pl.id
where lower(s.realm) = 'go'
-- and upper(pl.provider) = 'WEB'
and s.status in ('ACTIVE','CANCELED')
group by all


-- COMMAND ----------

-- DBTITLE 1,Migrated MT DTC Transactions
with new_mt_plans as (
  select * from dplus_finint_prod.silver.fi_priceplan_enriched 
  where lower(realm) = 'go'
  and lower(internalName) like '%legmt.%' OR lower(internalName) like '%motortrend discovery%'
  and upper(provider) = 'WEB'
  order by createDate desc
)

select 
t.*,
pl.provider,
pl.price / 100 as plan_price,
pl.currency as plan_currency,
pl.priceplantype,
pl.period as plan_period,
pl.internalname,
pl.market as plan_market

from dplus_finint_prod.silver.fi_transaction_enriched t
join new_mt_plans pl on t.priceplanid = pl.id
where t.realm = 'go'
and upper(pl.provider) = 'WEB'

