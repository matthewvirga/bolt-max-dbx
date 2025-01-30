# File used for storing commonly used functions
import logging
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, 
    explode_outer, 
    coalesce, 
    when, 
    lit, 
    upper
)
from pyspark.sql.types import StructType, ArrayType

# Optional: Configure logging
logging.basicConfig(level=logging.INFO)

def source_tables_combined(entity_config, read_function):
    """
    Combines all source tables for an entity into a single DataFrame, selecting only record.* and kafkaHeaders.*.

    Args:
        entity_config (dict): Configuration for the entity, including source tables.
        read_function (function): Function to read the source tables (e.g., spark.read.format("delta").table).

    Returns:
        DataFrame: A combined DataFrame with only the desired fields.
    """
    combined_df = None

    for table_name in entity_config["source_tables"]:
        print(f"Reading table: {table_name}")
        try:
            # Extract market from the table name
            match = re.search(r"\.raw_\w+_(\w+)_", table_name)
            if not match:
                raise ValueError(f"Table name '{table_name}' does not match expected pattern '.raw_<tenant>_<market>_...'")
            
            market = match.group(1)
            print(f"Extracted market: {market} from table name: {table_name}")

            # Read the table and select the required fields
            region_df = (
                read_function(table_name)
                .selectExpr("market", "tenant", "record.transaction.*", "kafkaHeaders.*")
            )

            # Fill null values for 'market' and set 'tenant' to 'beam'
            region_df = region_df.withColumn(
                "market",
                when(col("market").isNull(), lit(market)).otherwise(col("market"))
            ).withColumn(
                "tenant",
                lit("beam")  # Always replace tenant with 'beam'
            )

            # Union the DataFrames
            combined_df = region_df if combined_df is None else combined_df.union(region_df)
        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            raise e

    if combined_df is None:
        raise ValueError("No tables were combined; check the source_tables configuration.")
    
    return combined_df

# Functions for the source data
def flatten_dataframe(df):
    """
    Recursively flattens a DataFrame by exploding arrays and expanding structs to columns.

    Args:
        df (DataFrame): Input PySpark DataFrame with nested arrays and structs.
        
    Returns:
        DataFrame: Fully flattened DataFrame.
    """
    while True:
        # Identify array and struct columns
        array_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
        struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
        
        # If no arrays or structs remain, break the loop
        if not array_cols and not struct_cols:
            break
        
        # Explode arrays if any
        if array_cols:
            df = df.withColumn(array_cols[0], explode_outer(col(array_cols[0])))
        
        # Expand structs if any
        if struct_cols:
            struct_field = struct_cols[0]
            expanded_cols = [f"{struct_field}.{nested_field.name}" for nested_field in df.schema[struct_field].dataType.fields]
            df = df.select(
                "*",
                *[col(nested_col).alias(nested_col.replace(".", "_")) for nested_col in expanded_cols]
            ).drop(struct_field)
    
    return df
# Functions as it relates to each entity for ETL

def dfs_payments_V1(df):
    """
    Final Transformations for payment entities to land in the dlt table silver layer
    """
    return df.select(
        # event_id
        when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), \
            col("stateTransitions_refunded_refundReference"))
        .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), \
            col("stateTransitions_chargeback_providerChargebackReference"))
        .otherwise(col("id")).alias("event_id"),

        # original_transaction_id
        when(col("stateTransitions_refunding_providerRefundReference").isNotNull(), col("id"))
        .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), col("id"))
        .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), col("id"))
        .alias("original_transaction_id"),

        # Directly select other fields
        coalesce(col("x_wbd_tenant"), lit("beam")).alias("wbd_tenant"),
        coalesce(col("x_wbd_user_home_market"), col("market")).alias("wbd_region"),
        col("userId"),
        col("created").alias("created_date"),
        col("stateTransitions_occurred").alias("event_occurred_date"),
        col("source_type").alias("source_type"),
        col("source_reference").alias("source_reference"),

        # billed_amount
        when(col("stateTransitions_refunding_source").isNotNull(), coalesce(
            col("stateTransitions_refunding_amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_simpleAmount_minorAmount") * -1
        )).when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), coalesce(
            col("stateTransitions_refunded_amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_simpleAmount_minorAmount") * -1
        )).when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), coalesce(
            col("stateTransitions_chargeback_amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
            col("amountDetails_simpleAmount_minorAmount") * -1
        )).otherwise(coalesce(
            col("amountDetails_amountWithTax_minorAmountInclTax"),
            col("amountDetails_simpleAmount_minorAmount")
        )).alias("billed_amount"),

        # Tax_amount
        when(col("stateTransitions_refunding_source").isNotNull(), coalesce(
            col("stateTransitions_refunding_amountDetails_amountWithTax_taxMinorAmount") * -1,
            col("amountDetails_amountWithTax_taxMinorAmount") * -1
        )).when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), coalesce(
            col("stateTransitions_refunded_amountDetails_amountWithTax_taxMinorAmount") * -1,
            col("amountDetails_amountWithTax_taxMinorAmount") * -1
        )).when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), coalesce(
            col("stateTransitions_chargeback_amountDetails_amountWithTax_taxMinorAmount") * -1,
            col("amountDetails_amountWithTax_taxMinorAmount") * -1
        )).otherwise(coalesce(col("amountDetails_amountWithTax_taxMinorAmount"), lit(0))).alias("tax_amount"),
        col("amountDetails_currencyCode").alias("currencyCode"),
        (col("amountDetails_amountWithTax_taxRateMinorUnits") / 10000).alias("tax_rate"),
        col("amountDetails_amountWithTax_components_description").alias("tax_description"),
        upper(col("amountDetails_amountWithTax_taxationCountryCode")).alias("taxation_country_code"),

        # event_type
        when(col("stateTransitions_pending_providerPaymentReference").isNotNull(), lit("PENDING"))
        .when(col("stateTransitions_successful_providerPaymentReference").isNotNull(), lit("SUCCESSFUL"))
        .when(col("stateTransitions_canceled_reason").isNotNull(), lit("CANCELLED"))
        .when((col("stateTransitions_failed_providerPaymentReference").isNotNull()) | (col("stateTransitions_failed_reason").isNotNull()), lit("FAILED"))
        .when((col("stateTransitions_retrying_nextRetry").isNotNull()) | (col("stateTransitions_retrying_reason").isNotNull()), lit("RETRYING"))
        .when(col("stateTransitions_refunding_source").isNotNull(), lit("REFUNDING"))
        .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), lit("REFUNDED"))
        .when(col("stateTransitions_refundFailed_reason").isNotNull(), lit("REFUND FAILED"))
        .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), lit("CHARGEBACK"))
        .when(col("stateTransitions_timedOut_emptyObject").isNotNull(), lit("TIMED OUT"))
        .when(col("stateTransitions_revoked_reason").isNotNull(), lit("REVOKED"))
        .when(col("stateTransitions_chargebackRejected_providerChargebackReference").isNotNull(), lit("CHARGEBACK REJECTED"))
        .when((col("stateTransitions_corrected_providerPaymentReference").isNotNull()) | (col("stateTransitions_corrected_providerRefundReference").isNotNull()), lit("CORRECTED"))
        .otherwise(lit("CREATED")).alias("event_type"),

        # provider_Reference_id
        when(col("stateTransitions_pending_providerPaymentReference").isNotNull(), col("stateTransitions_pending_providerPaymentReference"))
        .when(col("stateTransitions_successful_providerPaymentReference").isNotNull(), col("stateTransitions_successful_providerPaymentReference"))
        .when(col("stateTransitions_failed_providerPaymentReference").isNotNull(), col("stateTransitions_failed_providerPaymentReference"))
        .when(col("stateTransitions_refunding_providerRefundReference").isNotNull(), col("stateTransitions_refunding_providerRefundReference"))
        .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), col("stateTransitions_refunded_providerRefundReference"))
        .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), col("stateTransitions_chargeback_providerChargebackReference"))
        .when(col("stateTransitions_chargebackRejected_providerChargebackReference").isNotNull(), col   ("stateTransitions_chargebackRejected_providerChargebackReference"))
        .when(col("stateTransitions_corrected_providerPaymentReference").isNotNull(), col("stateTransitions_corrected_providerPaymentReference"))
        .when(col("stateTransitions_corrected_providerRefundReference").isNotNull(), col("stateTransitions_corrected_providerRefundReference"))
        .alias("provider_reference_id"),

        # Additional fields
        col("paymentMethodId"),
        col("legacyExportingSource"),
        col("installmentDetails_numberOfInstallments").alias("paymentInstallments"),
        col("merchantAccount"),
        col("items_subscriptionDetails_servicePeriod_startDate").alias("service_period_startdate"),
        col("items_subscriptionDetails_servicePeriod_endDate").alias("service_period_enddate"),
        col("stateTransitions_retrying_nextRetry").alias("next_retry_date")
    )

def dfs_payments_v2(df):
    return df
    # return df.select(
    #     # event_id
    #     when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), \
    #         col("stateTransitions_refunded_refundReference"))
    #     .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), \
    #         col("stateTransitions_chargeback_providerChargebackReference"))
    #     .otherwise(col("id")).alias("event_id"),

    #     # original_transaction_id
    #     when(col("stateTransitions_refunding_providerRefundReference").isNotNull(), col("id"))
    #     .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), col("id"))
    #     .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), col("id"))
    #     .alias("original_transaction_id"),

    #     # Directly select other fields
    #     coalesce(col("x_wbd_tenant"), lit("beam")).alias("wbd_tenant"),
    #     coalesce(col("x_wbd_user_home_market"), col("market")).alias("wbd_region"),
    #     col("userId"),
    #     col("created").alias("created_date"),
    #     col("stateTransitions_occurred").alias("event_occurred_date"),
    #     col("source_type").alias("source_type"),
    #     col("source_reference").alias("source_reference"),

    #     # billed_amount - need to enhance with the new "items" arrays
    #     when(col("stateTransitions_refunding_source").isNotNull(), coalesce(
    #         col("stateTransitions_refunding_amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_simpleAmount_minorAmount") * -1
    #     )).when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), coalesce(
    #         col("stateTransitions_refunded_amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_simpleAmount_minorAmount") * -1
    #     )).when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), coalesce(
    #         col("stateTransitions_chargeback_amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_amountWithTax_minorAmountInclTax") * -1,
    #         col("amountDetails_simpleAmount_minorAmount") * -1
    #     )).otherwise(coalesce(
    #         col("amountDetails_amountWithTax_minorAmountInclTax"),
    #         col("amountDetails_simpleAmount_minorAmount")
    #     )).alias("billed_amount"),

    #     # Tax_amount - need to enhance with the new "items" arrays
    #     when(col("stateTransitions_refunding_source").isNotNull(), coalesce(
    #         col("stateTransitions_refunding_amountDetails_amountWithTax_taxMinorAmount") * -1,
    #         col("amountDetails_amountWithTax_taxMinorAmount") * -1
    #     )).when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), coalesce(
    #         col("stateTransitions_refunded_amountDetails_amountWithTax_taxMinorAmount") * -1,
    #         col("amountDetails_amountWithTax_taxMinorAmount") * -1
    #     )).when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), coalesce(
    #         col("stateTransitions_chargeback_amountDetails_amountWithTax_taxMinorAmount") * -1,
    #         col("amountDetails_amountWithTax_taxMinorAmount") * -1
    #     )).otherwise(coalesce(col("amountDetails_amountWithTax_taxMinorAmount"), lit(0))).alias("tax_amount"),
    #     col("amountDetails_currencyCode").alias("currencyCode"),
    #     (col("amountDetails_amountWithTax_taxRateMinorUnits") / 10000).alias("tax_rate"),
    #     col("amountDetails_amountWithTax_components_description").alias("tax_description"),
    #     upper(col("amountDetails_amountWithTax_taxationCountryCode")).alias("taxation_country_code"),

    #     # event_type
    #     when(col("stateTransitions_pending_providerPaymentReference").isNotNull(), lit("PENDING"))
    #     .when(col("stateTransitions_successful_providerPaymentReference").isNotNull(), lit("SUCCESSFUL"))
    #     .when(col("stateTransitions_canceled_reason").isNotNull(), lit("CANCELLED"))
    #     .when((col("stateTransitions_failed_providerPaymentReference").isNotNull()) | (col("stateTransitions_failed_reason").isNotNull()), lit("FAILED"))
    #     .when((col("stateTransitions_retrying_nextRetry").isNotNull()) | (col("stateTransitions_retrying_reason").isNotNull()), lit("RETRYING"))
    #     .when(col("stateTransitions_refunding_source").isNotNull(), lit("REFUNDING"))
    #     .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), lit("REFUNDED"))
    #     .when(col("stateTransitions_refundFailed_reason").isNotNull(), lit("REFUND FAILED"))
    #     .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), lit("CHARGEBACK"))
    #     .when(col("stateTransitions_timedOut_emptyObject").isNotNull(), lit("TIMED OUT"))
    #     .when(col("stateTransitions_revoked_reason").isNotNull(), lit("REVOKED"))
    #     .when(col("stateTransitions_chargebackRejected_providerChargebackReference").isNotNull(), lit("CHARGEBACK REJECTED"))
    #     .when((col("stateTransitions_corrected_providerPaymentReference").isNotNull()) | (col("stateTransitions_corrected_providerRefundReference").isNotNull()), lit("CORRECTED"))
    #     .otherwise(lit("CREATED")).alias("event_type"),

    #     # provider_Reference_id
    #     when(col("stateTransitions_pending_providerPaymentReference").isNotNull(), col("stateTransitions_pending_providerPaymentReference"))
    #     .when(col("stateTransitions_successful_providerPaymentReference").isNotNull(), col("stateTransitions_successful_providerPaymentReference"))
    #     .when(col("stateTransitions_failed_providerPaymentReference").isNotNull(), col("stateTransitions_failed_providerPaymentReference"))
    #     .when(col("stateTransitions_refunding_providerRefundReference").isNotNull(), col("stateTransitions_refunding_providerRefundReference"))
    #     .when(col("stateTransitions_refunded_providerRefundReference").isNotNull(), col("stateTransitions_refunded_providerRefundReference"))
    #     .when(col("stateTransitions_chargeback_providerChargebackReference").isNotNull(), col("stateTransitions_chargeback_providerChargebackReference"))
    #     .when(col("stateTransitions_chargebackRejected_providerChargebackReference").isNotNull(), col   ("stateTransitions_chargebackRejected_providerChargebackReference"))
    #     .when(col("stateTransitions_corrected_providerPaymentReference").isNotNull(), col("stateTransitions_corrected_providerPaymentReference"))
    #     .when(col("stateTransitions_corrected_providerRefundReference").isNotNull(), col("stateTransitions_corrected_providerRefundReference"))
    #     .alias("provider_reference_id"),

    #     # Additional References
    #     col("provider"),
    #     col("billingAddressId"),
    #     col("type"),
    #     col()
    # )
