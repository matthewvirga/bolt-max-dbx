"""
This logic dynamically creates DLT silver tables in real-time 
for both regional/platform-specific and global entities.
"""

import dlt
from config import entity_configs, global_entity_configs
from utils import source_tables_combined, flatten_dataframe
from utils import (
    fs_payment_entities,
    fs_subscription_entities,
    fs_paymentmethod_entities,
    fs_user_entities,
    fs_product_entities,
    fs_priceplan_entities,
    fs_campaign_entities,
    fs_plans_entities,  # Add global transformation function here
)
from pyspark.sql.functions import col

# Map transformation functions to their respective final table names
transformation_functions = {
    "fs_payment_entities": fs_payment_entities,
    "fs_subscription_entities": fs_subscription_entities,
    "fs_paymentmethod_entities": fs_paymentmethod_entities,
    "fs_user_entities": fs_user_entities,
    "fs_product_entities": fs_product_entities,
    "fs_priceplan_entities": fs_priceplan_entities,
    "fs_campaign_entities": fs_campaign_entities,
}

# --- Loop 1: Regional/Platform-Specific Entities ---
def create_entity_pipeline(entity_name, entity_config):
    """
    Dynamically creates a DLT table for a regional/platform-specific entity.
    """
    @dlt.table(
        name=entity_config["final_transformations"],
        comment=f"Final table for {entity_name}",
        partition_cols=["region", "platform"]
    )
    def final_table():
        """
        Processes regional/platform-specific source tables, flattens, applies transformations, 
        and writes to the final table.
        """
        # Combine source tables for the entity
        combined_df = source_tables_combined(entity_config["source_tables"], dlt.read)
        
        # Flatten the combined DataFrame
        flattened_df = flatten_dataframe(combined_df)
        
        # Apply the transformation function for the entity
        transformation_fn = transformation_functions.get(
            entity_config["final_transformations"],
            lambda df: df  # Default to no-op if undefined
        )
        transformed_df = transformation_fn(flattened_df)
        
        return transformed_df

# Dynamically create pipelines for all entities
for entity_name, entity_config in entity_configs.items():
    create_entity_pipeline(entity_name, entity_config)

# --- Global Entities: Plans Table ---
@dlt.table(
    name="fs_plans_entities",
    comment="Final combined table for global entities: products, priceplans, and campaigns"
)
def fs_plans_entities_table():
    """
    Incrementally reads, flattens, joins, and transforms the products, 
    priceplans, and campaigns data, then writes to a final table.
    """

    # Incrementally read and flatten the source tables
    products_df = flatten_dataframe(dlt.read(global_entity_configs["raw_product_entities"]["source_table"]))
    priceplans_df = flatten_dataframe(dlt.read(global_entity_configs["raw_priceplan_entities"]["source_table"]))
    campaigns_df = flatten_dataframe(dlt.read(global_entity_configs["raw_campaign_entities"]["source_table"]))

    # Perform joins (adjust join keys as needed)
    joined_df = (
        priceplans_df
        .join(
            products_df,
            priceplans_df["record_priceplan_productid"] == products_df["record_product_id"],
            "left_outer"
        )
        .join(
            campaigns_df,
            priceplans_df["record_priceplan_id"] == campaigns_df["record_campaign_pricePlanId"],
            "left_outer"
        )
    )

    # Apply transformation logic
    transformed_df = fs_plans_entities(joined_df)

    return transformed_df