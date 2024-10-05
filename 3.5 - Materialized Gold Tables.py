# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books_sales.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"
# MAGIC
# MAGIC -- delta lake caching strategies

# COMMAND ----------

# in DB, the concept of materialized view is closed to the gold table

from pyspark.sql import functions as F

# summary statistic of ordrer
# we auto handling the late, out of ordering data using the watermark
query = (spark.readStream
                .table("books_sales")
                .withWatermark("order_timestamp", "10 minutes")
                .groupBy(
                    F.window("order_timestamp", "5 minutes").alias("time"),"author")
                .agg(
                    F.count("order_id").alias("orders_count"),
                    F.avg("quantity").alias("avg_quantity"))
                .writeStream
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                    .trigger(availableNow=True)
                    .table("authors_stats")
                )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- review the new gold table
# MAGIC SELECT * FROM authors_stats

# COMMAND ----------


