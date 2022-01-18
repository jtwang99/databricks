-- Databricks notebook source
-- MAGIC %md # Diving into Delta Lake: Unpacking the Transaction Log V2
-- MAGIC 
-- MAGIC This notebook refers to the following [blog](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) and [tech talk](https://databricks.zoom.us/webinar/register/WN_E6u5D7NQSpCdWsEk-u8IGA); it is a modified version of the [Delta Lake Tutorial: Spark + AI Summit 2019 EU](https://github.com/delta-io/delta/tree/master/examples/tutorials/saiseu19) and [Using SQL to Query your Data Lake with Delta Lake](https://github.com/databricks/tech-talks/blob/master/2020-09-30%20%7C%20Using%20SQL%20to%20Query%20your%20Data%20Lake%20with%20Delta%20Lake/Using%20SQL%20to%20Query%20Your%20Data%20Lake%20With%20Delta%20Lake.dbc).
-- MAGIC <br/>&nbsp;
-- MAGIC 
-- MAGIC ### Steps to run this notebook
-- MAGIC 
-- MAGIC You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well.
-- MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 6.1 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>&nbsp;
-- MAGIC 
-- MAGIC ### Source Data for this notebook
-- MAGIC 
-- MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

-- COMMAND ----------

-- MAGIC %md <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
-- MAGIC 
-- MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
-- MAGIC 
-- MAGIC * **Open format**: Stored as Parquet format in blob storage.
-- MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
-- MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
-- MAGIC * **Audit History**: History of all the operations that happened in the table.
-- MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
-- MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
-- MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
-- MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Import Data and create pre-Delta Lake Table
-- MAGIC 
-- MAGIC * This will create a lot of small Parquet files emulating the typical small file problem that occurs with streaming or highly transactional data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC spark.sql("SET spark.databricks.delta.convert.metadataCheck.enabled = false")
-- MAGIC spark.sql("SET spark.databricks.delta.properties.defaults.checkpoint.writeStatsAsStruct = true")
-- MAGIC 
-- MAGIC # Configure location of loanstats_2012_2017.parquet
-- MAGIC lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
-- MAGIC 
-- MAGIC # Read loanstats_2012_2017.parquet
-- MAGIC data = spark.read.parquet(lspq_path)
-- MAGIC 
-- MAGIC # Reduce the amount of data (to run on DBCE)
-- MAGIC (loan_stats, loan_stats_rest) = data.randomSplit([0.01, 0.99], seed=123)
-- MAGIC 
-- MAGIC # Select only the columns needed
-- MAGIC loan_stats = loan_stats.select("addr_state", "loan_status")
-- MAGIC 
-- MAGIC # Create loan by state
-- MAGIC loan_by_state = loan_stats.groupBy("addr_state").count()
-- MAGIC loan_by_state = loan_by_state.withColumn("stream_no", lit(0))
-- MAGIC loan_by_state.createOrReplaceTempView("loan_by_state")
-- MAGIC 
-- MAGIC # Configure paths
-- MAGIC DELTALAKE_PATH = "/ml/loan_by_state"
-- MAGIC PARQUET_PATH = "/ml/loan_by_state_parquet"
-- MAGIC 
-- MAGIC # Remove folder if it exists
-- MAGIC dbutils.fs.rm(DELTALAKE_PATH, recurse=True)
-- MAGIC dbutils.fs.rm(PARQUET_PATH, recurse=True)
-- MAGIC 
-- MAGIC # Create Parquet tables
-- MAGIC loan_by_state.write.parquet(DELTALAKE_PATH)
-- MAGIC loan_by_state.write.parquet(PARQUET_PATH)
-- MAGIC 
-- MAGIC # Display Parquet table files
-- MAGIC display(dbutils.fs.ls(DELTALAKE_PATH))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Display table
-- MAGIC display(loan_by_state)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Easily Convert Parquet to Delta Lake format
-- MAGIC 
-- MAGIC With Delta Lake, you can easily transform your Parquet data into Delta Lake format. 

-- COMMAND ----------

-- Convert Parquet table to Delta
CONVERT TO DELTA parquet.`/ml/loan_by_state`;

-- COMMAND ----------

-- Describe details
DESCRIBE DETAIL delta.`/ml/loan_by_state`;

-- COMMAND ----------

-- MAGIC %md ### Review File System
-- MAGIC What's the difference between a Parquet table vs. Delta Lake table?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Review PARQUET_PATH folder
-- MAGIC display(dbutils.fs.ls(PARQUET_PATH))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Review DELTALAKE_PATH folder
-- MAGIC display(dbutils.fs.ls(DELTALAKE_PATH))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Review DELTALAKE_PATH folder
-- MAGIC display(dbutils.fs.ls(DELTALAKE_PATH + "/_delta_log/"))

-- COMMAND ----------

-- MAGIC %md For more information about the transaction log, refer to the **Diving into Delta Lake Series**, [Unpacking the Transaction Log](https://databricks.com/diving-into-delta-lake-talks/unpacking-transaction-log)

-- COMMAND ----------

-- MAGIC %md ### Configure Streaming Functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import random
-- MAGIC import os
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC 
-- MAGIC 
-- MAGIC def random_checkpoint_dir(): 
-- MAGIC   return "/tmp/dennyglee/sais_eu_19_demo/chkpt/%s" % str(random.randint(0, 10000))
-- MAGIC 
-- MAGIC # User-defined function to generate random state
-- MAGIC states = ["CA", "TX", "NY", "IA"]
-- MAGIC 
-- MAGIC @udf(returnType=StringType())
-- MAGIC def random_state():
-- MAGIC   return str(random.choice(states))
-- MAGIC 
-- MAGIC # Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
-- MAGIC def generate_and_append_data_stream(table_format, table_path, stream_no):
-- MAGIC   
-- MAGIC   stream_data = spark.readStream.format("rate").option("rowsPerSecond", 10).load() \
-- MAGIC     .withColumn("addr_state", random_state()) \
-- MAGIC     .withColumn("count", (rand() * 10).cast("long")) \
-- MAGIC     .withColumn("stream_no", lit(stream_no)) \
-- MAGIC     .select("addr_state", "count", "stream_no") 
-- MAGIC 
-- MAGIC   query = stream_data.writeStream \
-- MAGIC     .format(table_format) \
-- MAGIC     .option("mergeSchema", True) \
-- MAGIC     .option("checkpointLocation", random_checkpoint_dir()) \
-- MAGIC     .trigger(processingTime = "10 seconds") \
-- MAGIC     .start(table_path)
-- MAGIC 
-- MAGIC   return query
-- MAGIC 
-- MAGIC # Function to stop all streaming queries 
-- MAGIC def stop_all_streams():
-- MAGIC   # Stop all the streams
-- MAGIC   print("Stopping all streams")
-- MAGIC   for s in spark.streams.active:
-- MAGIC     s.stop()
-- MAGIC   print("Stopped all streams")
-- MAGIC   print("Deleting checkpoints")  
-- MAGIC   dbutils.fs.rm("/tmp/dennyglee/sais_eu_19_demo/chkpt/", True)
-- MAGIC   print("Deleted checkpoints")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Stop the notebook before the streaming cell, in case of a "run all" 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("stop") 

-- COMMAND ----------

-- MAGIC %md ## Review our Parquet and Delta Lake Tables

-- COMMAND ----------

SELECT * FROM parquet.`/ml/loan_by_state_parquet`

-- COMMAND ----------

SELECT * FROM delta.`/ml/loan_by_state`

-- COMMAND ----------

-- MAGIC %md **Observation**: Notice that there isn't any superficial difference between the two tables; dive deeper to spot the differences.

-- COMMAND ----------

-- MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified Batch and Streaming Source and Sink
-- MAGIC 
-- MAGIC Because Delta Lake contains ACID transactions, it can unify batch and streaming!
-- MAGIC * To showcase our raw ingestion of Bronze data, let's first start with Parquet

-- COMMAND ----------

-- MAGIC %md ### Streaming with Parquet

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's start our first stream
-- MAGIC stream_query = generate_and_append_data_stream(
-- MAGIC     table_format = "parquet", 
-- MAGIC     table_path = PARQUET_PATH,
-- MAGIC     stream_no = 1)

-- COMMAND ----------

-- MAGIC %md ### Important
-- MAGIC **Wait** until the stream is up and running before executing the next cell below.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's review the count of data
-- MAGIC spark.read.format("parquet").load(PARQUET_PATH).count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # What happens if we start a second stream?
-- MAGIC stream_query2 = generate_and_append_data_stream(
-- MAGIC     table_format = "parquet", 
-- MAGIC     table_path = PARQUET_PATH,
-- MAGIC     stream_no = 2)

-- COMMAND ----------

-- MAGIC %md **Observation**: Note, while the first stream is able to insert data into the table, the second stream isn't doing anything at all.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Stop All Streams
-- MAGIC stop_all_streams()

-- COMMAND ----------

-- MAGIC %md ### Streaming with Delta Lake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's start our first stream
-- MAGIC stream_query = generate_and_append_data_stream(
-- MAGIC     table_format = "delta", 
-- MAGIC     table_path = DELTALAKE_PATH,
-- MAGIC     stream_no = 1)

-- COMMAND ----------

-- MAGIC %md ### Important
-- MAGIC **Wait** until the stream is up and running before executing the next cell below.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's review the count of data
-- MAGIC spark.read.format("delta").load(DELTALAKE_PATH).count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's start our second stream
-- MAGIC stream_query2 = generate_and_append_data_stream(
-- MAGIC     table_format = "delta", 
-- MAGIC     table_path = DELTALAKE_PATH,
-- MAGIC     stream_no = 2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's start our third stream
-- MAGIC stream_query3 = generate_and_append_data_stream(
-- MAGIC     table_format = "delta", 
-- MAGIC     table_path = DELTALAKE_PATH,
-- MAGIC     stream_no = 3)

-- COMMAND ----------

-- MAGIC %md **Observation**: Note, all three streams are able to insert data into the Delta Lake table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Stop All Streams
-- MAGIC stop_all_streams()

-- COMMAND ----------

-- MAGIC %md ## Review the Streams

-- COMMAND ----------

-- Parquet 
SELECT stream_no, count(1) FROM parquet.`/ml/loan_by_state_parquet` GROUP BY stream_no

-- COMMAND ----------

-- Delta Lake
SELECT stream_no, count(1) FROM delta.`/ml/loan_by_state` GROUP BY stream_no ORDER BY stream_no

-- COMMAND ----------

-- MAGIC %md ### Observations
-- MAGIC * Notice how the Parquet table has only one stream which is odd as there should be two - streams 0 (the initial stream) and stream 1 (the new stream).  
-- MAGIC * This happened because there was a change in schema such that the new files overran the older files thus corrupting the table
-- MAGIC * With Delta Lake, you can see there are all three streams, the initial stream 0 and three streams - 1, 2, and 3 that were running concurrently in Delta Lake (that couldn't run in Parquet)

-- COMMAND ----------

-- MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Audit Delta Lake Table History
-- MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#history).

-- COMMAND ----------

DESCRIBE HISTORY delta.`/ml/loan_by_state`;

-- COMMAND ----------

OPTIMIZE delta.`/ml/loan_by_state`

-- COMMAND ----------

VACUUM delta.`/ml/loan_by_state`

-- COMMAND ----------

DESCRIBE HISTORY delta.`/ml/loan_by_state`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/ml/loan_by_state"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/ml/loan_by_state/_delta_log/"))

-- COMMAND ----------

-- MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Read the Delta Transaction Log 
-- MAGIC * Review the JSON
-- MAGIC * Review the Checkpoint

-- COMMAND ----------

-- MAGIC %md #### Review JSON Example

-- COMMAND ----------

-- MAGIC %python
-- MAGIC j0 = spark.read.json("/ml/loan_by_state/_delta_log/00000000000000000001.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Commit Information
-- MAGIC display(j0.select("commitInfo").where("commitInfo is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Add Information
-- MAGIC display(j0.select("add").where("add is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Transaction Information
-- MAGIC display(j0.select("txn").where("txn is not null"))

-- COMMAND ----------

-- MAGIC %md #### CRC file 
-- MAGIC * Contains `tableSizeInBytes` and `numFiles` (for Dynamic Partition Pruning)

-- COMMAND ----------

-- MAGIC %sh head /dbfs/ml/loan_by_state/_delta_log/00000000000000000001.crc

-- COMMAND ----------

-- MAGIC %md #### Review Checkpoint Example

-- COMMAND ----------

-- MAGIC %python
-- MAGIC chkpt0 = spark.read.parquet("/ml/loan_by_state/_delta_log/00000000000000000010.checkpoint.parquet")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Protocol Information
-- MAGIC display(chkpt0.select("protocol").where("protocol is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Add Information
-- MAGIC display(chkpt0.select("add").where("add is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Metadata Information
-- MAGIC display(chkpt0.select("metaData").where("metaData is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Remove Information
-- MAGIC display(chkpt0.select("remove").where("remove is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Transaction Information
-- MAGIC display(chkpt0.select("txn").where("txn is not null"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("DESCRIBE TABLE EXTENDED denny_db.loan_by_state").show()

-- COMMAND ----------

DESCRIBE DETAIL denny_db.loan_by_state;

-- COMMAND ----------

DESCRIBE TABLE denny_db.loan_by_state;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED denny_db.loan_by_state;

-- COMMAND ----------

-- MAGIC %md #Join the community!
-- MAGIC 
-- MAGIC 
-- MAGIC * [Delta Lake on GitHub](https://github.com/delta-io/delta)
-- MAGIC * [Delta Lake Slack Channel](https://delta-users.slack.com/) ([Registration Link](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA))
-- MAGIC * [Public Mailing List](https://groups.google.com/forum/#!forum/delta-users)
