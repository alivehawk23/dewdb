# Databricks notebook source
# Use dbutils secrets to get Snowflake credentials.
# Correct usage of dbutils.secrets.get
#user = dbutils.secrets.get(scope="data-warehouse", key="balajis3")
#password = dbutils.secrets.get(scope="data-warehouse", key="Sivan$evadi2024")

options = {
  "sfUrl": "https://srb80301.us-east-1.snowflakecomputing.com",
  "sfUser": "balajis3",
  "sfPassword": "Sivan$evadi2024",
  "sfDatabase": "HMAPDB",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
spark.range(5).write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ADS_DEV.T_RECIP_MBI_DIM") \
  .save()

# COMMAND ----------

# Read the data written by the previous cell back.
# Unpack the dictionary to keyword arguments
df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ADS_DEV.T_RECIP_MBI_DIM") \
  .load()

display(df)

# COMMAND ----------



# COMMAND ----------

# Write the data to a Delta table

df.write.format("delta").saveAsTable("sf_ingest_table")
