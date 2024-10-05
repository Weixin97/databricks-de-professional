# Databricks notebook source
# MAGIC %run ../Databricks-Certified-Data-Engineer-Professional/Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REDACT SENSITIVE FIELD WHEN THE USER MEMBER IS ADMIN
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customer_vw AS 
# MAGIC   SELECT 
# MAGIC       customer_id,
# MAGIC       CASE 
# MAGIC         WHEN is_member('admins_demo') THEN email 
# MAGIC         ELSE 'REDACTED'
# MAGIC       END AS email,
# MAGIC       gender,
# MAGIC       CASE
# MAGIC         WHEN is_member('admins_demo') THEN first_name
# MAGIC         ELSE 'REDACTED'
# MAGIC       END AS first_name,
# MAGIC       CASE
# MAGIC         WHEN is_member('admins_demo') THEN last_name 
# MAGIC         ELSE 'REDACTED'
# MAGIC       END AS last_name,
# MAGIC       CASE 
# MAGIC         WHEN is_member('admins_demo') THEN street 
# MAGIC         ELSE 'REDACTED'
# MAGIC       END AS street,
# MAGIC       city,
# MAGIC       country,
# MAGIC       row_time 
# MAGIC     FROM customers_silver
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM customer_vw 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- row level access control
# MAGIC
# MAGIC CREATE OR REPLACE VIEW customers_fr_vw AS 
# MAGIC SELECT * FROM customer_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('admins_demo') THEN TRUE 
# MAGIC     ELSE country = "France" AND row_time > "2022-01-01"
# MAGIC   END

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from customers_fr_vw

# COMMAND ----------


