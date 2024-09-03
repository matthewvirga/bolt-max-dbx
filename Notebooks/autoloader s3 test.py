# Databricks notebook source
# DBTITLE 1,Autoloader Script with parameters
# Access parameters passed from the Workflow UI
s3_bucket_path = dbutils.widgets.get("s3_bucket_path")
table_name = dbutils.widgets.get("table_name")
schema_location = dbutils.widgets.get("schema_location")
checkpoint_location = dbutils.widgets.get("checkpoint_location")

# Read from S3 bucket using Auto Loader
df = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true") 
            .option("inferSchema", "true")
            .option("cloudFiles.schemaLocation", schema_location)
            .load(s3_bucket_path))

# Modify column names in the schema
new_column_names = [col.replace(' ', '_') for col in df.schema.names]
df = df.toDF(*new_column_names)

# Write to Delta table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(table_name))
