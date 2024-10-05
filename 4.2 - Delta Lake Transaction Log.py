# Databricks notebook source
# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM bronze 

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.parquet("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000010.json"))

# COMMAND ----------


