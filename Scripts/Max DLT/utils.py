# File used for storing commonly used functions
import logging
from pyspark.sql.functions import col, explode_outer
from pyspark.sql import DataFrame

# Optional: Configure logging
logging.basicConfig(level=logging.INFO)

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

def source_tables_combined(source_tables: dict, read_function):
    """
    Combines all source tables into a single DataFrame with region and platform identifiers.

    Args:
        source_tables (dict): A nested dictionary of regions and platforms mapping to table names.
        read_function (function): Function to read the table (e.g., dlt.read or spark.read.format).

    Returns:
        DataFrame: A combined DataFrame with region and platform identifiers.
    """
    combined_df = None

    for region, platforms in source_tables.items():
        for platform, table_name in platforms.items():
            try:
                # Skip placeholders (None or empty strings)
                if not table_name:
                    logging.warning(f"Skipping region: {region.upper()}, platform: {platform.upper()} (No table provided)")
                    continue

                logging.info(f"Processing table: {table_name} (Region: {region.upper()}, Platform: {platform.upper()})")
                
                # Read the table and add region and platform identifiers
                region_df = (
                    read_function(table_name)
                    .selectExpr(
                        "record.transaction.*",
                        f"'{region.upper()}' as region",
                        f"'{platform.upper()}' as platform"
                    )
                )

                # Union the data
                combined_df = region_df if combined_df is None else combined_df.union(region_df)

            except Exception as e:
                logging.error(f"Failed to process table: {table_name} (Region: {region.upper()}, Platform: {platform.upper()}) - {e}")
                raise e

    return combined_df

    # Functions as it relates to each entity for ETL

    def fs_payment_entities(df):
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
            col("region"),
            col("platform"),
            col("userId"),
            col("created").alias("created_date"),
            col("event.occurred").alias("event_occurred_date"),
            col("source.type").alias("source_type"),
            col("source.reference").alias("source_reference"),

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

            # Additional fields
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
            .when((col("stateTransitions_corrected_providerPaymentReference").isNotNull()) | (col("stateTransitions_corrected_providerRefundReference").isNotNull()), lit   ("CORRECTED"))
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
    
    def fs_subscription_entities():
        """
        Final Transformations for subscription entities to land in the dlt table silver layer
        """
        return df.select(

        )

    def fs_paymentmethod_entities():
        """
        Final Transformations for paymentmethod entities to land in the dlt table silver layer
        """
        return df.select(
            
        )

    def fs_user_entities():
        """
        Final Transformations for user entities to land in the dlt table silver layer
        """
        return df.select(
            
        )
    
    def fs_plan_entities():
        """
        Final Transformations for payment entities to land in the dlt table silver layer
        """
        return df.select(
            
        )









