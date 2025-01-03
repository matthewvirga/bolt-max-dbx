# Databricks notebook source
# Create widgets for input parameters
dbutils.widgets.text("bronze_table_amer", "bolt_dcp_prod.bronze.raw_s2s_payment_method_entities", "Bronze Table (AMER)")
dbutils.widgets.text("bronze_table_latam", "bolt_dcp_prod.any_latam_bronze.raw_s2s_payment_method_entities", "Bronze Table (LATAM)")
dbutils.widgets.text("bronze_table_emea", "bolt_dcp_prod.any_emea_bronze.raw_s2s_payment_method_entities", "Bronze Table (EMEA)")
dbutils.widgets.text("silver_table", "bolt_finint_int.silver.fs_paymentmethods", "Silver Table")
dbutils.widgets.text("checkpoint_location", "/mnt/delta/bolt_finint_int/silver/fs_paymentmethods/_checkpoint", "Checkpoint Location")
dbutils.widgets.text("merge_key", "paymentMethodId", "Merge Key")

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.appName("StreamPaymentMethods").getOrCreate()

# Retrieve widget values
bronze_table_amer = dbutils.widgets.get("bronze_table_amer")
bronze_table_latam = dbutils.widgets.get("bronze_table_latam")
bronze_table_emea = dbutils.widgets.get("bronze_table_emea")
silver_table = dbutils.widgets.get("silver_table")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
merge_key = dbutils.widgets.get("merge_key")

# Define streaming DataFrame from the source tables
global_paymentmethod_entities = (
    spark.readStream
    .format("delta")
    .table(bronze_table_amer)
    .selectExpr("tenant", "record.paymentMethod.*", "'AMER' as region")
    .union(
        spark.readStream
        .format("delta")
        .table(bronze_table_latam)
        .selectExpr("tenant", "record.paymentMethod.*", "'LATAM' as region")
    )
    .union(
        spark.readStream
        .format("delta")
        .table(bronze_table_emea)
        .selectExpr("tenant", "record.paymentMethod.*", "'EMEA' as region")
    )
)

# Create a temporary view for SQL
global_paymentmethod_entities.createOrReplaceTempView("global_paymentmethod_entities_temp")

# Define the SQL query for ETL
result = spark.sql("""
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

    from global_paymentmethod_entities_temp
    where id is not null
""")

# Define the target Delta table
target_table = DeltaTable.forName(spark, silver_table)

# Perform the merge operation
result.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .foreachBatch(lambda batch_df, batch_id: (
        target_table.alias("t")
        .merge(
            batch_df.alias("s"),
            f"t.{merge_key} = s.{merge_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )) \
    .start()
