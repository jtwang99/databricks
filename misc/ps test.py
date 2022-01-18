# Databricks notebook source
# MAGIC %pip install pyspark-pandas

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps
pdf = pd.DataFrame({"a": [1, 3, 5]})  # pandas DataFrame
sdf = spark.createDataFrame(pdf)  # PySpark DataFrame
psdf = sdf.to_pandas_on_spark()  # pandas-on-Spark DataFrame
# Query via SQL
ps.sql("SELECT count(*) as num FROM {psdf}")

# COMMAND ----------

loan_pdf=ps.sql("SELECT * FROM delta.`/ml/loan_by_state`")

# COMMAND ----------

loan_pdf
