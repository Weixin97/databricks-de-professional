# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_ID STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze 
# MAGIC   WHERE topic = "orders" )

# COMMAND ----------

# convert the above logic to a streaming view, convert static table to streaming temp view
# this allow us to write streaming logic using spark sql

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v 
# MAGIC   FROM bronze_tmp 
# MAGIC   WHERE topic = "orders"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- switch sql to python, using the temp view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS 
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v 
# MAGIC   FROM bronze_tmp 
# MAGIC   WHERE topic = "orders"
# MAGIC )

# COMMAND ----------

# now, we use the streaming write to persist the result of the streaming temp view to disk
# notice, we use trigger availableNow, so all records will be processed in multiple microbatches until no more data is available and then stop this stream

query = (spark.table("orders_silver_tmp")
                .writeStream
                .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                .trigger(availableNow=True)
                .table("orders_silver"))
query.awaitTermination()


# COMMAND ----------

# refactor our logic using python syntax instead of SQL

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
         .filter("topic = 'orders'")
         .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
         .select("v.*")
        .writeStream
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
         .trigger(availableNow=True)
         .table("orders_silver"))
query.awaitTermination()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------


