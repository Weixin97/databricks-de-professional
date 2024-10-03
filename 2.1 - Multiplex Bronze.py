# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
    # configuring the stream to use autoloader by using cloudFiles format
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                    .writeStream
                        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                        .option("mergeSchema", True) # leverage the schema evalution 
                        .partitionBy("topic", "year_month")
                        .trigger(availableNow=True)
                        .table("bronze")
                        )

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze") #
display(batch_df) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- alternatively, use sql to query the table
# MAGIC SELECT * FROM bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

# copy new data file to source directory
bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze

# COMMAND ----------


