# Databricks notebook source
# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can define constraints on existing delta table using ALTER TABLE ADD CONSTRAINT command , followed by the boolean condition of the check
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- purposely add records that break the constraint
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ACID guarantee of DELTA LAKE ensure atomic
# MAGIC -- none of the records are inserted 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
# MAGIC
# MAGIC -- the command fail, the constraint statement will verify that all existing rows satisfy the constraint before adding it to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE quantity <= 0

# COMMAND ----------

# if we recall our logic as previous lecturer, to deal with this, we can filter out the rows with 0 during ingestion
from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to drop constraint
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)

# COMMAND ----------


