from pyspark.sql.functions import when, upper

# Load the tables into Dataframes
product = spark.table("bolt_finint_prod.silver.fi_product_enriched")
priceplan = spark.table("bolt_finint_prod.silver.fi_priceplanv2_enriched")
campaign = spark.table("bolt_finint_prod.silver.fi_campaignv2_enriched")


# Join the tables based on the provided hints
joined_df = priceplan.join(product, priceplan.productId == product.id, 'inner') \
                        .join(campaign, priceplan.campaignId == campaign.id, 'left_outer')

# Select the desired columns, explicitly specifying the DataFrame source
plans = joined_df.select(
    priceplan.realm,
    priceplan.id.alias("priceplan_id"),
    when(upper(product.name).like("%MVPD%"), "Partner")
    .when(priceplan.pricePlanType == "TYPE_THIRD_PARTY", "IAP")
    .otherwise("Direct")
    .alias("sub_source"),
    (priceplan.price / 100).alias("plan_price"),
    priceplan.currency,
    priceplan.period.substr(8,99).alias("payment_period"),
    priceplan.retentionOffer,
    when(priceplan.provider != "WEB", priceplan.provider)
    .otherwise("Direct")
    .alias("provider_name"),
    priceplan.campaignId,
    priceplan.marketId.substr(11, 99).alias("market"),
    priceplan.productId,
    priceplan.internalName,
    product.name.alias("product_name"),
    product.description.alias("product_description"),
    product.tierType.substr(11,99).alias("tier_type"),
    campaign.name.alias("campaign_name")
)