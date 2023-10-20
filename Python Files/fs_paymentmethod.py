from pyspark.sql.functions import upper

# Load the table into a DataFrame
pm = spark.table("bolt_finint_prod.silver.fi_paymentmethodv2_enriched")

# Apply the transformations and select the desired columns
paymentmethod = pm.select(
    pm.id.alias("paymentmethodid"),
    pm.provider.substr(18,99).alias("payment_provider"),
    upper(pm.cardDetails.cardProvider).alias("card_Brand"),
    upper(pm.cardDetails.fundingSource).alias("card_funding"),
    pm.cardDetails.cardIssuingBank.alias("issuing_bank"),
    pm.countryCode.alias("payment_country"),
    pm.geoLocation.countryIso.alias("country_iso")
)
