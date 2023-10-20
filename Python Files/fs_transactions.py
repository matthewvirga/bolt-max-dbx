from pyspark.sql.functions import udf, when, expr, col
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
    extract_successful_ref_udf(col("stateTransitions"))) \
        .withColumn("transaction_status", transaction_status_udf(col("currentState"))) \
        .withColumn("transaction_type", 
                when(col("currentState.refunded.providerRefundReference").isNotNull(), "REFUND")
                .when(col("currentState.chargeback.providerChargebackReference").isNotNull(), "CHARGEBACK")
                .otherwise("CHARGE"))

transactions.show()
