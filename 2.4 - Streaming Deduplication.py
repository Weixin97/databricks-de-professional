# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- many source system like kafka introducing duplicated records
# MAGIC -- we apply deduplication at silver - layer
# MAGIC -- remember, the bronze table should retain a history of the true state of our streaming source.
# MAGIC -- this prevent potential data loss due to applying aggressive equality enforcement and pre-processing at the initial ingestion, and also helps minimizing data latency during ingestion

# COMMAND ----------

# let's read the bronze data using read method
(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# with the static data, we can simply apply the drop duplicate method 

from pyspark.sql import functions as F 

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                      )

# COMMAND ----------

print(batch_total)

# COMMAND ----------

# structured streaming can track state information also for the unique keys in the data
# this ensure duplicate records do not exist in the micro batchs
# however overtime, this state info will scale to represent all history
# we can limit the amount of the state to be maintained by using watermarking
deduped_df =  (spark.readStream
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .withWatermark("order_timestamp", "30 seconds") # watermarking allows to only track state info for a window of time in which we expect records could be delayed.
                      .dropDuplicates(["order_id", "order_timestamp"])
                      )

# COMMAND ----------

# when dealing with streaming duplication, there's another level of complexity compared to static data
# as each micro batch is processed, we need to ensure that records to be inserted are not already in the target table
# we can achive this using merge insert

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    sql_query = """
        MERGE INTO orders_silver a
        USING orders_microbatch b
        ON a.order_id = b.order_id
        WHEN NOT MATCHED THEN INSERT *
    """

    # spark.sql(sql_query)
    # instead, we can access the local spark session from the microBatchDF data frame
    microBatchDF.sparkSession.sql(sql_query)
    # if spark session below 10.5 ,
    # microBatchDF._jdf.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ensure the table exist before merge insert 
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

# now, in order to code the upsert function in our stream, we need to use the foreachBatch method
# this provide the option to execute custom data writing logic on each micro batch of a streaming data.

query = (deduped_df.writeStream
                    .foreachBatch(upsert_data)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                    .trigger(availableNow=True)
                    .start()
                    )
query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()
print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
