## Bolt tables for RAW data prior to Finint
---
### Global (not split by Market):
---
bolt_payments_prod.gold.s2s_campaign_entities

bolt_payments_prod.gold.s2s_price_plan_entities

bolt_payments_prod.gold.s2s_product_catalog_entities

### Latam:
---
#### Untokenized data:

bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_method_entities

bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_transaction_entities

bolt_dcp_prod.beam_latam_bronze.raw_s2s_audit_events

bolt_dcp_prod.beam_latam_bronze.raw_s2s_profile_entities

bolt_dcp_prod.beam_latam_bronze.raw_s2s_subscription_entities

bolt_dcp_prod.beam_latam_bronze.raw_s2s_user_capabilities_entities

bolt_dcp_prod.beam_latam_bronze.raw_s2s_user_entities

bolt_dcp_prod.beam_latam_gold.s2s_audit_events

bolt_gauth_prod.any_latam_bronze.raw_s2s_gauth_audit_events

#### Tokenized Data:

bolt_subscriptions_prod.any_latam_gold.s2s_subscription_payment_failure_events

bolt_gauth_prod.beam_latam_gold.s2s_user_capabilities_entities


### US:
---
#### Untokenized data:

bolt_dcp_prod.bronze.raw_s2s_audit_events

bolt_dcp_prod.bronze.raw_s2s_legal_events

bolt_dcp_prod.bronze.raw_s2s_payment_method_entities

bolt_dcp_prod.bronze.raw_s2s_payment_transaction_entities

bolt_dcp_prod.bronze.raw_s2s_profile_entities

bolt_dcp_prod.bronze.raw_s2s_subscription_entities

bolt_dcp_prod.bronze.raw_s2s_user_capabilities_entities

bolt_gauth_prod.bronze.raw_s2s_gauth_audit_events

#### Tokenized Data:

bolt_subscriptions_prod.gold.s2s_subscription_payment_failure_events

bolt_gauth_prod.gold.s2s_capability_definition_entities

---
## Bolt Real time FinInt Tables - Silver
---
### Global (not split by Market):
---
bolt_finint_prod.silver.fi_campaignv2_enriched

bolt_finint_prod.silver.fi_priceplanv2_enriched

bolt_finint_prod.silver.fi_priceplanv2_enriched


### US:
---
#### Untokenized data:
bolt_finint_prod.silver.fi_subscriptionv2_enriched

bolt_finint_prod.silver.fi_transactionv2_enriched

bolt_finint_prod.silver.fi_paymentmethodv2_enriched

### LATAM:
---
#### Untokenized data:
bolt_finint_prod.latam_silver.fi_subscriptionv2_enriched

bolt_finint_prod.latam_silver.fi_transactionv2_enriched

bolt_finint_prod.latam_silver.fi_paymentmethodv2_enriched

