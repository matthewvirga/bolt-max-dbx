# Databricks notebook source
# DBTITLE 1,Create Main Dataframes
from pyspark.sql.functions import when, col, coalesce, lit, DataFrame

def load_and_transform_table(table_name: str, selected_columns: list) -> DataFrame:
    # Load the table into a DataFrame
    df = spark.table(table_name)
    
    # Apply the transformations
    transformed_df = df.select(*selected_columns)
    
    return transformed_df

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

# Define the desired transformations and selections - Transactions
trx_cols = [

]

# Define the desired transformations and selections - Priceplans
pp_cols = [

]

# Define the desired transformations and selections - Products
pr_cols = [

]

# Define the desired transformations and selections - Campaigns
cam_cols = [

]

# Define the desired transformations and selections - Payment Methods
pm_cols = [

]

# Load the tables with transformations

subscription = load_and_transform_table("bolt_finint_prod.silver.fi_subscriptionv2_enriched", subs_cols)
transaction = load_and_transform_table("bolt_finint_prod.silver.fi_transactionv2_enriched", trx_cols)
priceplan = load_and_transform_table("bolt_finint_prod.silver.fi_priceplan_enriched_kafka", pp_cols)
product = load_and_transform_table("bolt_finint_prod.silver.fi_product_enriched", pr_cols)
campaign = load_and_transform_table("bolt_finint_prod.silver.fi_campaignv2_enriched", cam_cols)
paymentmethod = load_and_transform_table("bolt_finint_prod.silver.fi_paymentmethodv2_enriched", pm_cols)

display(subscription)
