# Databricks notebook source
# Load tables needed into Dataframes
subscription = spark.table("bolt_finint_prod.silver.fi_subscriptionv2_enriched")
transaction = spark.table("bolt_finint_prod.silver.fi_transactionv2_enriched")
priceplan = spark.table("bolt_finint_prod.silver.fi_priceplan_enriched_kafka")
product = spark.table("bolt_finint_prod.silver.fi_product_enriched")
campaign = spark.table("bolt_finint_prod.silver.fi_campaignv2_enriched")
paymentmethod = spark.table("bolt_finint_prod.silver.fi_paymentmethodv2_enriched")
