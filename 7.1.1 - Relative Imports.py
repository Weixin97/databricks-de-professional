# Databricks notebook source
from helper.cube import Cube

# COMMAND ----------

c2 = Cube(3)
c2.get_volume()

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls ./helper

# COMMAND ----------

import sys 
for path in sys.path :
    print(path)

# COMMAND ----------

import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

import sys 
for path in sys.path :
    print(path)

# COMMAND ----------


