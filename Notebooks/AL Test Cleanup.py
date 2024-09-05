# Databricks notebook source
# MAGIC %md
# MAGIC # Delete Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists bolt_finint_int.bronze.apple_sales_reports_s3

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists bolt_finint_int.bronze.amazon_sales_reports_s3

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists bolt_finint_int.bronze.google_sales_reports_s3

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists bolt_finint_int.bronze.roku_transactions_reports_s3

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup checkpoints and schemas

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/delta/bolt_finint_int/bronze/", True)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/delta/bolt_finint_int/bronze/")
for file in files:
    print(file.path)
