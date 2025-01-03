# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, explode_outer, upper, regexp_replace, lit

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Transactions Conversion") \
    .getOrCreate()

# Reading the dataset into a DataFrame
df = spark.read.table("bolt_finint_prod.beam_emea_silver.fi_transactionv2_enriched")

# Exploding stateTransitions and joining back to the main DataFrame
df = df.withColumn("event", explode_outer(col("stateTransitions")))

# Defining the transformed DataFrame
transactions_df = df.select(
    when(col("event.refunded.providerRefundReference").isNotNull(), col("event.refunded.refundReference"))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), col("event.chargeback.providerChargebackReference"))
    .otherwise(col("id")).alias("event_id"),

    when(col("event.refunding.providerRefundReference").isNotNull(), col("id"))
    .when(col("event.refunded.providerRefundReference").isNotNull(), col("id"))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), col("id")).alias("original_transaction_id"),

    col("realm"),
    col("userId"),
    col("created").alias("created_date"),
    col("event.occurred").alias("event_occurred_date"),
    col("source.type").alias("source_type"),
    col("source.reference").alias("source_reference"),

    when(col("event.refunded.providerRefundReference").isNotNull(), coalesce(col("event.refunded.amountDetails.amountWithTax.minorAmountInclTax") * -1, col("amountDetails.amountWithTax.minorAmountInclTax") * -1))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), coalesce(col("event.chargeback.amountDetails.amountWithTax.minorAmountInclTax") * -1, col("amountDetails.amountWithTax.minorAmountInclTax") * -1))
    .otherwise(coalesce(col("amountDetails.amountWithTax.minorAmountInclTax"), col("amountDetails.simpleAmount.minorAmount"))).alias("charged_amount"),

    when(col("event.refunded.providerRefundReference").isNotNull(), coalesce(col("event.refunded.amountDetails.currencyCode"), col("amountDetails.currencyCode")))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), coalesce(col("event.chargeback.amountDetails.currencyCode"), col("amountDetails.currencyCode")))
    .otherwise(col("amountDetails.currencyCode")).alias("currency"),

    when(col("event.refunded.providerRefundReference").isNotNull(), coalesce(col("event.refunded.amountDetails.amountWithTax.taxMinorAmount") * -1, coalesce(col("amountDetails.amountWithTax.taxMinorAmount") * -1, lit(0))))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), coalesce(col("event.chargeback.amountDetails.amountWithTax.taxMinorAmount") * -1, coalesce(col("amountDetails.amountWithTax.taxMinorAmount") * -1, lit(0))))
    .otherwise(coalesce(col("amountDetails.amountWithTax.taxMinorAmount"), lit(0))).alias("tax_amount"),

    when(
        col("event.refunded.providerRefundReference").isNotNull(), 
        coalesce(
            col("event.refunded.amountDetails.amountWithTax.components").getItem(0)["description"],
            col("amountDetails.amountWithTax.components").getItem(0)["description"]
        )
    ).when(
        col("event.chargeback.providerChargebackReference").isNotNull(),
        coalesce(
            col("event.chargeback.amountDetails.amountWithTax.components").getItem(0)["description"],
            col("amountDetails.amountWithTax.components").getItem(0)["description"]
        )
    ).otherwise(
        col("amountDetails.amountWithTax.components").getItem(0)["description"]
    ).alias("tax_description"),

    when(col("event.refunded.providerRefundReference").isNotNull(), coalesce(col("event.refunded.amountDetails.amountWithTax.taxRateMinorUnits") / 100, col("amountDetails.amountWithTax.taxRateMinorUnits") / 100))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), coalesce(col("event.chargeback.amountDetails.amountWithTax.taxRateMinorUnits") / 10000, col("amountDetails.amountWithTax.taxRateMinorUnits") / 100))
    .otherwise(col("amountDetails.amountWithTax.taxRateMinorUnits") / 100).alias("tax_rate"),

    when(col("event.refunded.providerRefundReference").isNotNull(), upper(coalesce(col("event.refunded.amountDetails.amountWithTax.taxationCountryCode"), col("amountDetails.amountWithTax.taxationCountryCode"))))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), upper(col("event.chargeback.amountDetails.amountWithTax.taxationCountryCode")))
    .otherwise(upper(col("amountDetails.amountWithTax.taxationCountryCode"))).alias("tax_country_code"),

    when(col("event.pending.providerPaymentReference").isNotNull(), "PENDING")
    .when(col("event.successful.providerPaymentReference").isNotNull(), "SUCCESSFUL")
    .when(col("event.canceled.reason").isNotNull(), "CANCELLED")
    .when((col("event.failed.providerPaymentReference").isNotNull() | col("event.failed.reason").isNotNull()), "FAILED")
    .when((col("event.retrying.nextretry").isNotNull() | col("event.retrying.reason").isNotNull()), "RETRYING")
    .when(col("event.refunding.source").isNotNull(), "REFUNDING")
    .when(col("event.refunded.providerRefundReference").isNotNull(), "REFUNDED")
    .when(col("event.refundFailed.reason").isNotNull(), "REFUND FAILED")
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), "CHARGEBACK")
    .when(col("event.timedOut.emptyObject").isNotNull(), "TIMED OUT")
    .when(col("event.revoked.reason").isNotNull(), "REVOKED")
    .when(col("event.chargebackRejected.providerChargebackReference").isNotNull(), "CHARGEBACK REJECTED")
    .when((col("event.corrected.providerPaymentReference").isNotNull() | col("event.corrected.providerRefundReference").isNotNull()), "CORRECTED")
    .otherwise("UNKNOWN").alias("event_type"),

    when(col("event.pending.providerPaymentReference").isNotNull(), col("event.pending.providerPaymentReference"))
    .when(col("event.successful.providerPaymentReference").isNotNull(), col("event.successful.providerPaymentReference"))
    .when(col("event.failed.providerPaymentReference").isNotNull(), col("event.failed.providerPaymentReference"))
    .when(col("event.refunding.providerRefundReference").isNotNull(), col("event.refunding.providerRefundReference"))
    .when(col("event.refunded.providerRefundReference").isNotNull(), col("event.refunded.providerRefundReference"))
    .when(col("event.chargeback.providerChargebackReference").isNotNull(), col("event.chargeback.providerChargebackReference"))
    .when(col("event.chargebackRejected.providerChargebackReference").isNotNull(), col("event.chargebackRejected.providerChargebackReference"))
    .when(col("event.corrected.providerPaymentReference").isNotNull(), col("event.corrected.providerPaymentReference"))
    .when(col("event.corrected.providerRefundReference").isNotNull(), col("event.corrected.providerRefundReference")).alias("provider_Reference_id"),

    when(col("event.canceled.reason").isNotNull(), col("event.canceled.reason"))
    .when(col("event.failed.reason").isNotNull(), col("event.failed.reason"))
    .when(col("event.retrying.reason").isNotNull(), col("event.retrying.reason"))
    .when(col("event.refundFailed.reason").isNotNull(), col("event.refundFailed.reason"))
    .when(col("event.revoked.reason").isNotNull(), col("event.revoked.reason"))
    .when(col("event.chargebackRejected.reason").isNotNull(), col("event.chargebackRejected.reason"))
    .when(col("event.timedOut.emptyObject").isNotNull(), col("event.timedOut.emptyObject")).alias("event_state_reason"),

    when(col("event.refunding.providerRefundReference").isNotNull(), col("event.refunding.source"))
    .when(col("event.refunded.providerRefundReference").isNotNull(), col("event.refunded.source")).alias("refund_source"),

    regexp_replace(col("provider"), "PAYMENT_PROVIDER_", "").alias("payment_provider"),

    regexp_replace(col("type"), "PAYMENT_TRANSACTION_TYPE_", "").alias("payment_type"),
    
    col("paymentMethodId"),
    col("merchantAccount"),
    col("items").getItem(0)["subscriptionDetails"]["serviceperiod"]["startdate"].alias("service_period_startdate"),
    col("items").getItem(0)["subscriptionDetails"]["serviceperiod"]["enddate"].alias("service_period_enddate"),
    col("event.retrying.nextretry").alias("next_retry_date")
)

# Creating a temporary view
transactions_df.createOrReplaceTempView("Transactions")



# COMMAND ----------

transactions_df.display()


# COMMAND ----------

from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

# Read the data from the existing table in Databricks
df = spark.table("bolt_payments_prod.any_latam_gold.s2s_payment_transaction_events")

# Flatten the nested JSON structure and extract failure reasons
df_flattened = df.select(
    col("userId").alias("user_id"),
    col("unpackedValue.transaction.invoiceId").alias("invoice_id"),
    col("unpackedValue.transaction.items")[0]["subscriptionDetails"]["servicePeriod"]["startDate"].alias("service_period_start"),
    col("unpackedValue.transaction.items")[0]["subscriptionDetails"]["servicePeriod"]["endDate"].alias("service_period_end"),
    col("unpackedValue.failedRetryably.state.reason").alias("failed_retryably_reason"),
    col("unpackedValue.failedTerminally.state.reason").alias("failed_terminally_reason"),
    col("unpackedValue.exhaustedRetries.state.reason").alias("exhausted_retries_reason"),
    col("unpackedValue.transaction.id").alias("transaction_id"),
    col("updatedAt").alias("attempt_timestamp")
)

# Determine event type and failure reason
df_final = df_flattened.withColumn(
    "event_type",
    when(col("failed_retryably_reason").isNotNull(), "RETRYING")
    .when((col("failed_terminally_reason").isNotNull()) | (col("exhausted_retries_reason").isNotNull()), "FAILED")
    .otherwise("UNKNOWN")
).withColumn(
    "failure_reason",
    when(col("failed_retryably_reason").isNotNull(), col("failed_retryably_reason"))
    .when(col("failed_terminally_reason").isNotNull(), col("failed_terminally_reason"))
    .when(col("exhausted_retries_reason").isNotNull(), col("exhausted_retries_reason"))
    .otherwise("")
)

# Add row number to each attempt for each user and invoice
window_spec = Window.partitionBy("user_id", "invoice_id").orderBy("attempt_timestamp")
df_final = df_final.withColumn("attempt_number", row_number().over(window_spec))

# Filter for relevant event types and order the results
df_result = df_final.filter(col("event_type").isin("RETRYING", "FAILED")).orderBy("user_id", "invoice_id", "attempt_number")

# Display the result
display(df_result)

# Optionally, show the schema for verification
df_result.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a CTE to flatten the nested JSON structure and extract failure reasons
# MAGIC WITH flattened_data AS (
# MAGIC     SELECT
# MAGIC         userId AS user_id,
# MAGIC         unpackedValue.transaction.invoiceId AS invoice_id,
# MAGIC         unpackedValue.transaction.items[0].subscriptionDetails.servicePeriod.startDate AS service_period_start,
# MAGIC         unpackedValue.transaction.items[0].subscriptionDetails.servicePeriod.endDate AS service_period_end,
# MAGIC         unpackedValue.failedRetryably.state.reason AS failed_retryably_reason,
# MAGIC         unpackedValue.failedTerminally.state.reason AS failed_terminally_reason,
# MAGIC         unpackedValue.exhaustedRetries.state.reason AS exhausted_retries_reason,
# MAGIC         unpackedValue.transaction.id AS transaction_id,
# MAGIC         unpackedValue.transaction.created AS attempt_timestamp
# MAGIC     FROM bolt_payments_prod.any_latam_gold.s2s_payment_transaction_events
# MAGIC     where unpackedValue.transaction.currentState is not NULL
# MAGIC       and unpackedValue.transaction.statetransitions is not NULL
# MAGIC ),
# MAGIC
# MAGIC -- Determine event type and failure reason
# MAGIC event_data AS (
# MAGIC     SELECT
# MAGIC         user_id,
# MAGIC         invoice_id,
# MAGIC         service_period_start,
# MAGIC         service_period_end,
# MAGIC         transaction_id,
# MAGIC         attempt_timestamp,
# MAGIC         -- Determine event type based on failure reasons
# MAGIC         CASE
# MAGIC             WHEN failed_retryably_reason IS NOT NULL THEN 'SOFT_FAILURE'
# MAGIC             WHEN failed_terminally_reason IS NOT NULL OR exhausted_retries_reason IS NOT NULL THEN 'HARD_FAILURE'
# MAGIC             ELSE 'UNKNOWN'
# MAGIC         END AS event_type,
# MAGIC         -- Determine failure reason based on failure reasons
# MAGIC         CASE
# MAGIC             WHEN failed_retryably_reason IS NOT NULL THEN failed_retryably_reason
# MAGIC             WHEN failed_terminally_reason IS NOT NULL THEN failed_terminally_reason
# MAGIC             WHEN exhausted_retries_reason IS NOT NULL THEN exhausted_retries_reason
# MAGIC             ELSE ''
# MAGIC         END AS failure_reason
# MAGIC     FROM flattened_data
# MAGIC ),
# MAGIC
# MAGIC -- Add row number to each attempt for each user and invoice
# MAGIC numbered_attempts AS (
# MAGIC     SELECT
# MAGIC         user_id,
# MAGIC         invoice_id,
# MAGIC         service_period_start,
# MAGIC         service_period_end,
# MAGIC         transaction_id,
# MAGIC         attempt_timestamp,
# MAGIC         event_type,
# MAGIC         failure_reason,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY user_id, service_period_start::date
# MAGIC             ORDER BY attempt_timestamp
# MAGIC         ) AS attempt_number
# MAGIC     FROM event_data
# MAGIC     WHERE event_type IN ('SOFT_FAILURE', 'HARD_FAILURE')
# MAGIC )
# MAGIC
# MAGIC -- Select the final result set, ordered by user, invoice, and attempt number
# MAGIC SELECT
# MAGIC     user_id,
# MAGIC     invoice_id,
# MAGIC     service_period_start,
# MAGIC     service_period_end,
# MAGIC     attempt_number,
# MAGIC     attempt_timestamp,
# MAGIC     event_type,
# MAGIC     failure_reason
# MAGIC FROM numbered_attempts
# MAGIC where service_period_start >= '2024-04-01'
# MAGIC and user_id = '1701782222356249717'
# MAGIC ORDER BY user_id, invoice_id, attempt_number
