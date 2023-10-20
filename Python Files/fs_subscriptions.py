from pyspark.sql.functions import when, col, coalesce, lit

# Define the desired transformations and selections - Subscriptions
subs_cols = [
    "globalSubscriptionId",
    "userId",
    col("status").substr(8, 99).alias("subscription_status"),
    col("startDate").cast("date").alias("start_date"),
    "pricePlanId",
    col("nextRenewalDate").cast("date").alias("next_renewal_date"),
    col("endDate").cast("date").alias("end_date"),
    col("cancellationDate").cast("date").alias("cancellation_date"),
    col("direct.affiliate").alias("affiliate"),
    when(col("direct.paymentProvider").isNull(), col("iap.Provider"))
        .otherwise(col("direct.paymentProvider"))
        .alias("payment_provider"),
    col("direct.subscriptionType").substr(6, 99).alias("subscription_type"),
    col("direct.affiliateStartDate").cast("date").alias("affiliate_start_date"),
    col("paymentMethodId").alias("payment_method_id"),
    coalesce(col("direct.campaignId"), col("iap.campaignId")).alias("campaign_id"),
    coalesce(col("iap.provider"), lit("Direct")).alias("provider"),
    col("terminationDate").cast("date").alias("termination_date"),
    "terminationReason",
    "terminationCode",
    "startedWithFreeTrial",
    col("nextRetryDate").cast("date").alias("next_retry_date"),
    "subscribedInTerritory",
    col("previousSubscriptionGlobalId").alias("prev_globalSubscriptionId"),
    "inFreeTrial",
    "subscribedInSite",
]

# Load the table into a DataFrame and apply the transformations
subscription = spark.table("bolt_finint_prod.silver.fi_subscriptionv2_enriched").select(*subs_cols)