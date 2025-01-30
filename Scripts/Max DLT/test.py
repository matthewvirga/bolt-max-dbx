import yaml
from utils_v2 import source_tables_combined, flatten_dataframe, dfs_payments_V2
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestEntityPipeline").getOrCreate()

# Load YAML configuration
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Choose environment and entity
environment = "int"  # Change to "prod" for production
entity_name = "payments_v2"  # Entity to test
entity_config = config["environment"][environment]["entities"][entity_name]

# Run source_tables_combined
print(f"\nRunning source_tables_combined for entity: {entity_name}")
combined_df = source_tables_combined(entity_config, spark.read.format("delta").table)
print("\nSchema after source_tables_combined:")
combined_df.printSchema()
print("\nSample rows after source_tables_combined:")
combined_df.show(5)

# Run flatten_dataframe
print(f"\nRunning flatten_dataframe for entity: {entity_name}")
flattened_df = flatten_dataframe(combined_df)
print("\nSchema after flatten_dataframe:")
flattened_df.printSchema()
print("\nSample rows after flatten_dataframe:")
flattened_df.show(5)

# Run transformation function
print(f"\nRunning transformation function for entity: {entity_name}")
transformed_df = dfs_payments_V1(flattened_df)
print("\nSchema after transformation function:")
transformed_df.printSchema()
print("\nSample rows after transformation function:")
transformed_df.show(5)
