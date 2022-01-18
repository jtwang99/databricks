# Databricks notebook source
# MAGIC %sql SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + 1) as new_key

# COMMAND ----------

# MAGIC %sql SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v) as new_key

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.createDataFrame([(1, {"foo": -2.0, "bar": 2.0})], ("id", "data"))
df.select(transform_keys(
    "data", lambda k, _: upper(k)).alias("data_upper")
).show(truncate=False)

# COMMAND ----------

new_column='new_col'
df = spark.createDataFrame([(1, {"IT": 10.0, "SALES": 2.0, "OPS": 24.0})], ("id", "data"))
df.select(transform_values(
    "data", lambda k, v: when(k.isin("IT", "OPS"), v + 10.0).otherwise(v)
).alias(new_column)).show(truncate=False)


# COMMAND ----------

df

# COMMAND ----------

# MAGIC %sh wget https://github.com/rejetto/hfs2/releases/download/v2.4-rc07/hfs.exe 

# COMMAND ----------

# MAGIC %sh ls -l

# COMMAND ----------

# MAGIC %sql select current_user(), current_catalog(),current_database(),current_date(),current_timestamp(),current_timezone()

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

import json
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
#print(notebook_info)
print(json.dumps(notebook_info, indent=2))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/05-analyze-text", recurse=True)

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------


