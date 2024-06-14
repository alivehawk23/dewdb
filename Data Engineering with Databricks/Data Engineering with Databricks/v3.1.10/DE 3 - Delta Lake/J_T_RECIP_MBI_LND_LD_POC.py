# Databricks notebook source
# MAGIC %pip install snowflake-sqlalchemy pyspark
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark import SparkConf
# MAGIC
# MAGIC options = {
# MAGIC   "sfURL" : "https://srb80301.us-east-1.snowflakecomputing.com",
# MAGIC   "sfWarehouse" : "COMPUTE_WH",
# MAGIC   "sfDatabase" : "HMAPDB",
# MAGIC   "sfSchema" : "PUBLIC",
# MAGIC   "sfUser" : "balajis3",
# MAGIC   "sfPassword" : "Sivan$evadi2024"
# MAGIC }
# MAGIC
# MAGIC SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# MAGIC
# MAGIC df_member = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC   .options(**options) \
# MAGIC   .option("dbtable", "DLK_MES.T_BMS_MEMBER") \
# MAGIC   .load()
# MAGIC
# MAGIC #display(df_member)
# MAGIC
# MAGIC # Load the data first
# MAGIC df_mbr_medicare_idntfr = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC   .options(**options) \
# MAGIC   .option("dbtable", "DLK_MES.T_BMS_MBR_MEDICARE_IDNTFR") \
# MAGIC   .load()
# MAGIC
# MAGIC #display(df_mbr_medicare_idntfr)
# MAGIC
# MAGIC
# MAGIC #df_mbr_medicare_idntfr_selected= df_mbr_idntfr.select("MBR_SID","MEDICARE_BNFCRY_IDNTFR", "FROM_DATE", "TO_DATE")
# MAGIC
# MAGIC df_mbr_medicare_idntfr_filtered = df_mbr_medicare_idntfr.filter(
# MAGIC     "OPRTNL_FLAG = 'A' AND 20241106 BETWEEN FROM_DATE AND TO_DATE")
# MAGIC
# MAGIC df_mbr_idntfr = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC   .options(**options) \
# MAGIC   .option("dbtable", "DLK_MES.T_BMS_MBR_IDENTIFIER") \
# MAGIC   .load()
# MAGIC
# MAGIC
# MAGIC #df_mbr_idntfr_selected = df_mbr_idntfr.select("MBR_SID","IDNTFR", "FILE_NM", "TO_DATE")
# MAGIC
# MAGIC df_mbr_idntfr_filtered = df_mbr_idntfr.filter(
# MAGIC     "OPRTNL_FLAG = 'A' AND IDNTFR_TYPE_CID = '60' AND 20241106 BETWEEN FROM_DATE AND TO_DATE")
# MAGIC
# MAGIC
# MAGIC df_result = df_member.join(df_mbr_medicare_idntfr_filtered, "MBR_SID").join(df_mbr_idntfr_filtered, "MBR_SID")
# MAGIC
# MAGIC #df_result.show()
# MAGIC
# MAGIC display(df_result)
# MAGIC #display(df_mbr_medicare_idntfr_filtered)

# COMMAND ----------

# Read the data written by the previous cell back.
# Unpack the dictionary to keyword arguments
df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ADS_DEV.T_RECIP_MBI_DIM") \
  .load()

display(df)
