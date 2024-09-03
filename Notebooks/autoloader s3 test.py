# Databricks notebook source
from pyspark.sql.functions import lit

# Define the S3 bucket path
s3_bucket_path = "s3://aws-discovery-prod01/Roku/processed/roku_inbound/"

# Try to read a small amount of data from the S3 bucket to check access
try:
    df = (spark.read
                .format("csv")
                .option("header", "true")
                .load(s3_bucket_path)
                .limit(1))
    # If the read is successful, display a message indicating access is available
    access_check_df = spark.createDataFrame([("Access to the S3 bucket is available.",)], ["Status"])
except Exception as e:
    # If the read fails, display a message indicating access is not available
    access_check_df = spark.createDataFrame([("Access to the S3 bucket is not available.",)], ["Status"])

display(access_check_df)

# COMMAND ----------

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

# Write to Delta table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .outputMode("append")
    .table(table_name))
