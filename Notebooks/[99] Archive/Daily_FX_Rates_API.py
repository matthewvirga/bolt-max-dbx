# Databricks notebook source
import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO

# Fetch FX rates from an API (example)
api_url = 'https://api.exchangerate-api.com/v4/latest/USD'
response = requests.get(api_url)

# Check for a successful response
if response.status_code == 200:
    fx_data = response.json()
else:
    raise Exception("Failed to fetch FX data")

# Process the FX data
base_currency = fx_data['base']
date = fx_data['date']
rates = fx_data['rates']

# Convert rates dictionary to a DataFrame
fx_df = pd.DataFrame(list(rates.items()), columns=['Target Currency', 'Exchange Rate'])

# Add base currency and date columns
fx_df['Base Currency'] = base_currency
fx_df['Date'] = date
fx_df['Source'] = 'ExchangeRate-API'

# Reorder columns
fx_df = fx_df[['Date', 'Base Currency', 'Target Currency', 'Exchange Rate', 'Source']]

# Initialize S3 client without access keys
s3_client = boto3.client('s3', region_name='us-east-1')

bucket_name = 'aws-discovery-prod01'

# Create a date string for the filename
date_str = datetime.now().strftime('%Y%m%d')

# Modify the file path to include today's date
file_path = f'api_testing/fx_rates_{date_str}.csv'

# Save today's FX rates to S3
csv_buffer = StringIO()
fx_df.to_csv(csv_buffer, index=False)
s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=csv_buffer.getvalue())

print(f"Today's FX rates successfully saved to {file_path} in {bucket_name}")

# COMMAND ----------

# DBTITLE 1,Autoloader to Delta Table
from pyspark.sql.functions import col, regexp_replace

# Auto Loader configuration to read from S3 File to Dataframe
fx_s3_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "s3://aws-discovery-prod01/api_testing/autoloader_checkpoints/fx_al_schemas/")
    .option("cloudFiles.useStrictGlobber", "true")  # Enable strict globbing
    .option("pathGlobFilter", "*.csv")  # Use the path pattern from the widget
    .load("s3://aws-discovery-prod01/api_testing/")
)

# Write the DataFrame with renamed columns to a Delta table using Autoloader Streaming
query = (
    renamed_columns_df.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://aws-discovery-prod01/api_testing/autoloader_checkpoints/fx_al_checkpoint")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable('bolt_finint_int.bronze.fs_fx_rates')
)
