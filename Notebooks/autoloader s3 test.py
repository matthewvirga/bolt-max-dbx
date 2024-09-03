# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name

# Access parameters passed from the Workflow UI
s3_bucket_path = dbutils.widgets.get("s3_bucket_path")
table_name = dbutils.widgets.get("table_name")
schema_location = dbutils.widgets.get("schema_location")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
path_pattern = dbutils.widgets.get("path_pattern") # This is to determine which report to grab sales, finance, earnings etc.

# Read from S3 bucket using Auto Loader, filtering for files based on the path pattern
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true") 
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.includePathPatterns", path_pattern)  # Use the path pattern from the widget
    .option("cloudFiles.pathGlobFilter", path_pattern)  # Apply path pattern as glob filter for case sensitivity
    .load(s3_bucket_path))

# Modify column names in the schema
new_column_names = [col.replace(' ', '_') for col in df.schema.names]
df = df.toDF(*new_column_names)

# Add metadata columns
df_with_metadata = df.withColumn("appended_timestamp", current_timestamp()) \
                    .withColumn("file_name", input_file_name())

# Write to Delta table
streaming_query = (df_with_metadata.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(once=True)  # Trigger the job only once
    .start(table_name))
