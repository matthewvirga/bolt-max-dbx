# Databricks notebook source
import re
from pyspark.sql.functions import current_timestamp, input_file_name
import shutil

# Function to clean column names by replacing unsupported characters and spaces with "_"
def clean_column_names(df):
    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '_', c.replace(' ', '_')) for c in df.columns]
    return df.toDF(*new_columns)

# Access parameters passed from the Workflow UI
s3_bucket_path = dbutils.widgets.get("s3_bucket_path")
table_name = dbutils.widgets.get("table_name")
schema_location = dbutils.widgets.get("schema_location")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
path_pattern = dbutils.widgets.get("path_pattern")

# Temporary DBFS path for storing Parquet files
parquet_output_path = f"/dbfs/tmp/parquet-files/{table_name}"

# Step 1: Convert CSV files into Parquet files and store them temporarily in DBFS
df_csv = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")  # This will infer the schema better when writing to Parquet
    .option("cloudFiles.useStrictGlobber", "true")  # Enable strict globbing
    .option("pathGlobFilter", path_pattern)  # Use the path pattern from the widget
    .load(s3_bucket_path)
)

# Clean column names
df_cleaned = clean_column_names(df_csv)

# Write to temporary Parquet path in DBFS
df_cleaned.write.mode("overwrite").parquet(parquet_output_path)

# Step 2: Use Auto Loader to stream the Parquet files into Delta table

# Read the Parquet files from DBFS using Auto Loader
df_parquet_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # Parquet format now has proper schema
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.useStrictGlobber", "true")
    .load(parquet_output_path)
)

# Add deduplication and metadata columns
df_filtered = (
    df_parquet_stream
    .dropDuplicates()
.withColumn("load_timestamp", current_timestamp().cast("timestamp"))
)

# Write the deduplicated, timestamped Parquet data to Delta table
streamingQuery = (
    df_filtered.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(table_name)
)

# Step 3: Delete the Parquet files from DBFS after processing

# Wait for the stream to finish
streamingQuery.awaitTermination()

# After the stream has finished, delete the temporary Parquet files
dbutils.fs.rm(parquet_output_path, True)
