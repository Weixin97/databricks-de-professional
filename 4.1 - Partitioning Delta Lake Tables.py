# Databricks notebook source
# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql DESCRIBE TABLE EXTENDED bronze 

# COMMAND ----------

# list the file in this table directory
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze")
display(files)

# COMMAND ----------

# take a look in this partition directory
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------


