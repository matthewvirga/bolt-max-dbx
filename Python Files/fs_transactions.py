from pyspark.sql.functions import udf, when, expr, col, coalesce, lit, abs, cast
from pyspark.sql.types import StringType

# Load table into dataframe
transactions = spark.table("bolt_finint_prod.silver.fi_transactionv2_enriched")


# Define the UDF for prev vendor ref
def extract_successful_ref(state_transitions):
    for item in state_transitions:
        if item.successful:
            return item.successful.providerPaymentReference
    return None

# Define the Python function for transaction_status
def determine_transaction_status(currentState):
    # Check if the nested attributes are not None before accessing them
    success_conditions = [
        currentState.refunded.providerRefundReference if hasattr(currentState, 'refunded') and currentState.refunded else None,
        currentState.successful.providerPaymentReference if hasattr(currentState, 'successful') and currentState.successful else None,
        currentState.chargeback.providerChargebackReference if hasattr(currentState, 'chargeback') and currentState.chargeback else None,
        currentState.chargebackrejected.providerChargebackReference if hasattr(currentState, 'chargebackrejected') and currentState.chargebackrejected else None
    ]
    
    if any(success_conditions):
        return "SUCCESS"
    elif hasattr(currentState, 'pending') and currentState.pending and currentState.pending.providerPaymentReference:
        return "PENDING"
    elif hasattr(currentState, 'canceled') and currentState.canceled and currentState.canceled.reason:
        return "CANCELLED"
    elif (hasattr(currentState, 'failed') and currentState.failed and currentState.failed.providerPaymentReference) or \
         (hasattr(currentState, 'failed') and currentState.failed and currentState.failed.reason) or \
         (hasattr(currentState, 'retrying') and currentState.retrying and currentState.retrying.reason):
        return "FAILED"
    elif hasattr(currentState, 'revoked') and currentState.revoked and currentState.revoked.reason:
        return "REVOKED"
    else:
        return "ERROR"


# Create the UDFs
transaction_status_udf = udf(determine_transaction_status, StringType())
extract_successful_ref_udf = udf(extract_successful_ref, StringType())

# Apply the UDF to the transactions DataFrame
transactions = transactions.withColumn(
    "og_provider_payment_reference", 
    extract_successful_ref_udf(col("stateTransitions"))
) \
.withColumn(
    "transaction_status", 
    transaction_status_udf(col("currentState"))
) \
.withColumn(
    "transaction_type", 
    when(col("currentState.refunded.providerRefundReference").isNotNull(), "REFUND")
    .when(col("currentState.chargeback.providerChargebackReference").isNotNull(), "CHARGEBACK")
    .otherwise("CHARGE")
)

transactions = transactions.select(
    transactions["created"].cast("date").alias("created_date"),
    transactions["currentState.occurred"].cast("date").alias("occurred_date"),

    when(transactions["type"] == "PAYMENT_TRANSACTION_TYPE_AUTOMATED", "RECURRING")
    .when(transactions["type"] == "PAYMENT_TRANSACTION_TYPE_INTERACTIVE", "FIRST")
    .when((transactions["type"] == "PAYMENT_TRANSACTION_TYPE_INTERACTIVE") & (transactions["amountDetails.amountWithTax.minorAmountInclTax"] == 0),"FIRST_FREE")
    .otherwise("UNKNOWN")
    .alias("payment_type"),

    transactions["transaction_status"],
    transactions["transaction_type"],
    
    ((transactions["amountDetails.amountWithTax.minorAmountInclTax"] - coalesce(transactions["amountDetails.amountwithtax.taxminoramount"], lit(0))) / 100).alias("amount_before_tax"),
    
    (coalesce(transactions["amountDetails.amountwithtax.taxminoramount"], lit(0)) / 100).alias("sales_tax_amount"),
    
    when(transactions["currentState.refunded.providerRefundReference"].isNotNull() | 
    transactions["currentState.chargeback.providerChargebackReference"].isNotNull(), (abs(transactions["amountDetails.amountWithTax.minorAmountInclTax"])/ 100) * -1)
    .otherwise(abs(transactions["amountDetails.amountWithTax.minorAmountInclTax"]) / 100)
    .alias("charged_amount"),
    
    transactions["amountDetails.currencyCode"].alias("currency"),
    
    when(transactions["currentState.refunded.providerRefundReference"].isNotNull(),transactions["currentState.refunded.providerRefundReference"])
    .when(transactions["currentState.chargeback.providerChargebackReference"].isNotNull(),transactions["currentState.chargeback.providerChargebackReference"])
    .when(transactions["currentState.successful.providerPaymentReference"].isNotNull(),transactions["currentState.successful.providerPaymentReference"])
    .alias("provider_reference_id"),

    when(transactions["currentState.refunded.refundReference"].isNull(),transactions["currentState.chargeback.providerChargebackReference"])
    .otherwise(transactions["currentState.refunded.refundReference"])
    .alias("previous_trx_id"),

    transactions["currentState.refunded.source"].alias("Refund_source"),

    when(transactions["currentState.successful.providerPaymentReference"].isNotNull(),"")
    .otherwise(transactions["og_provider_payment_reference"])
    .alias("og_provider_reference_id"),

    transactions["provider"].substr(18,99).alias("payment_provider"),
    transactions["billingAddressid"].alias("billing_address_id"),
    transactions["userid"].alias("userid"),
    
    when(transactions["source.type"] == "subscription", transactions["source.reference"]).alias("global_subscription_id"),

    transactions["id"].alias("transaction_id")
    )

