import re
from pyspark.sql.functions import current_timestamp, input_file_name

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

# Define Schema for the CSV file
schema = StructType([
    StructField("Provider", StringType(), True),
    StructField("Provider_Country", StringType(), True),
    StructField("SKU", StringType(), True),
    StructField("Developer", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Version", StringType(), True),
    StructField("Product_Type_Identifier", StringType(), True),
    StructField("Units", IntegerType(), True),
    StructField("Developer_Proceeds", StringType(), True),
    StructField("Begin_Date", DateType(), True),
    StructField("End_Date", DateType(), True),
    StructField("Customer_Currency", StringType(), True),
    StructField("Country_Code", StringType(), True),
    StructField("Currency_of_Proceeds", StringType(), True),
    StructField("Apple_Identifier", StringType(), True),
    StructField("Customer_Price", DecimalType(10, 2), True),
    StructField("Promo_Code", StringType(), True),
    StructField("Parent_Identifier", StringType(), True),
    StructField("Subscription", StringType(), True),
    StructField("Period", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("CMB", StringType(), True),
    StructField("Device", StringType(), True),
    StructField("Supported_Platforms", StringType(), True),
    StructField("Proceeds_Reason", StringType(), True),
    StructField("Preserved_Pricing", StringType(), True),
    StructField("Client", StringType(), True),
    StructField("Order_Type", StringType(), True)
])

# Read from S3 bucket using Auto Loader, filtering for files based on the path pattern, adding autoloader metadata for tracking
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true") 
    .schema(schema)
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/delta/bolt_finint_int/bronze/apple_sales_reports_s3_test2/_schema")
    .option("cloudFiles.useStrictGlobber", "true")  # Enable strict globbing
    .option("pathGlobFilter", "*_SALES_*")  # Use the path pattern from the widget
    .load("s3://aws-discovery-prod01/Apple/processed/")
)

# Clean column names, de-dupe
df.dropDuplicates()  # Deduplicate records based on data content


# Add Timestamp for when the data was ingested
df_add_ts = (
    df.withColumn("load_timestamp", current_timestamp())  # Add timestamp column
)

# Write to Delta table
(
df_add_ts.writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/mnt/delta/bolt_finint_int/bronze/apple_sales_reports_s3_test2/_checkpoint")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # This will grab only the un-processed files then stop
    .toTable("bolt_finint_int.bronze.apple_sales_reports_s3_test2")
)