import dlt
from pyspark.sql.functions import col, explode_outer, coalesce, when, lit, round, upper
from pyspark.sql.types import StructType, ArrayType

# Configuration for source tables
source_tables = {
    "amer": "",
    "latam": "",
    "emea": "",
    "apac": ""
}

# Utility function for flattening DataFrame
def flatten_dataframe(df):
    """
    Recursively flattens a DataFrame by exploding arrays and expanding structs to columns.
    
    Args:
        df (DataFrame): Input PySpark DataFrame with nested arrays and structs.
        
    Returns:
        DataFrame: Fully flattened DataFrame.
    """
    while True:
        # Identify array columns to explode
        array_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
        
        # Identify struct columns to expand
        struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
        
        # If no array or struct columns remain, the DataFrame is fully flattened
        if not array_cols and not struct_cols:
            break
        
        # Explode the first array column (if present)
        if array_cols:
            col_to_explode = array_cols[0]
            df = df.withColumn(col_to_explode, explode_outer(col(col_to_explode)))
        
        # Expand the first struct column (if present)
        if struct_cols:
            col_to_expand = struct_cols[0]
            expanded_cols = [
                f"{col_to_expand}.{nested_field.name}" for nested_field in df.schema[col_to_expand].dataType.fields
            ]
            # Add expanded columns and drop the original struct column
            df = df.select(
                "*",
                *[col(nested_col).alias(nested_col.replace(".", "_")) for nested_col in expanded_cols]
            ).drop(col_to_expand)
    
    return df

@dlt.view
def source_tables_combined():
    """
    Combines all source tables into a single DataFrame with a region identifier.
    """
    combined_df = None
    for region, table in source_tables.items():
        region_df = (
            dlt.read(table)
            .selectExpr("record.transaction.*", f"'{region.upper()}' as region")
        )
        combined_df = region_df if combined_df is None else combined_df.union(region_df)
    return combined_df

@dlt.view
def flattened_source():
    """
    Flattens the combined source DataFrame.
    """
    source_df = dlt.read("source_tables_combined")
    flattened_df = flatten_dataframe(source_df)
    return flattened_df

@dlt.table(
    name="fs_payment_entities",
    comment="Global Payment Entities flattened and transformed for use by Financial Solutions team",
    partition_cols=["region"]
)
def final_table():
    """
    Transforms the flattened source DataFrame and writes to the Silver DLT table.
    """
    df = dlt.read("flattened_source")
    transformed_df = (
        df.withColumn()
    )
    return transformed_df