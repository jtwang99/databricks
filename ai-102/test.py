# Databricks notebook source
# MAGIC %sh ls -ls /root

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/ml

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/mnt/

# COMMAND ----------

# MAGIC %sh cp -R /root/AI-102-AIEngineer /dbfs/FileStore/AI-102-AIEngineer

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/user/hive/warehouse/store_data

# COMMAND ----------

dbutils.fs.ls('dbfs:/')

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore')

# COMMAND ----------

dbutils.fs.cp('file:/root/AI-102-AIEngineer', 'dbfs:/FileStore/ai-102', recurse=True)

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/ai-102')

# COMMAND ----------

dbutils.fs.mv('dbfs:/FileStore/01-getting-started', 'dbfs:/FileStore/ai-102', recurse=True)

# COMMAND ----------

dbutils.fs.cp()

# COMMAND ----------

# MAGIC %sh git clone https://github.com/MicrosoftLearning/AI-102-AIEngineer

# COMMAND ----------

# MAGIC %sh ls -l AI-102-AIEngineer

# COMMAND ----------

# MAGIC %fs ls FileStore/

# COMMAND ----------

# MAGIC %fs rm -r FileStore/readme.md

# COMMAND ----------


