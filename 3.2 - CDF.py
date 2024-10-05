# Databricks notebook source
# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes("customers_silver", 2)

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# COMMAND ----------

# in py, we can read the recorded data by adding 2 options to a read stream query, which are readChangeData, and the starting version
cdf_df = (spark.readStream
                .format("delta")
                .option("readChangeData", True)
                .option("startingVersion", 2)
                .table("customers_silver"))
display(cdf_df)

# COMMAND ----------

# observe the table directory
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver")
display(files)

# COMMAND ----------


