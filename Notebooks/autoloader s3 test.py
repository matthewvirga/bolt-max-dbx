# Databricks notebook source
import re
from pyspark.sql.functions import current_timestamp, input_file_name

# Function to clean column names by replacing unsupported characters and spaces with "_"
def clean_column_names(df):
    for c in df.columns:
        cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', c.replace(' ', '_'))
        df = df.withColumnRenamed(c, cleaned_name)
    return df

# Access parameters passed from the Workflow UI
s3_bucket_path = dbutils.widgets.get("s3_bucket_path")
table_name = dbutils.widgets.get("table_name")
schema_location = dbutils.widgets.get("schema_location")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
path_pattern = dbutils.widgets.get("path_pattern") # This is to determine which report to grab sales, finance, earnings etc.

# Read from S3 bucket using Auto Loader, filtering for files based on the path pattern, adding autoloader metadata for tracking
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true") 
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.useStrictGlobber", "true")  # Enable strict globbing
    .option("pathGlobFilter", path_pattern)  # Use the path pattern from the widget
    .load(s3_bucket_path)
    .withColumn("source_file", input_file_name())
    .withColumn("autoload_timestamp", current_timestamp())
)

# Clean column names, de-dupe
df_final = (
    clean_column_names(df)
    .dropDuplicates()  # Deduplicate records based on data content
)

# Write to Delta table
(
df_final.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # This will grab only the un-processed files then stop
    .toTable(table_name)
)
