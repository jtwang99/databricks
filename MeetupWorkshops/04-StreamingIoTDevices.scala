// Databricks notebook source
displayHTML(s"""
<h3>
  <img width="200px" src="https://spark.apache.org/images/spark-logo-trademark.png"/> 
  + 
  <img src="http://training.databricks.com/databricks_guide/databricks_logo_400px.png"/>
</h3>
""")

// COMMAND ----------

// MAGIC %md #Structured Streaming using Scala DataFrames API
// MAGIC 
// MAGIC Apache Spark 2.0 adds the first version of a new higher-level stream processing API, Structured Streaming. In this notebook we are going to take a quick look at how to use DataFrame API to build Structured Streaming queries. We want to compute real-time metrics like running counts and windowed counts on a stream of timestamped devcie events. Thesse events are randomly generated so there will be unpredictability in the data analysis, since it is computer generated data than real data. But it does not preclude us from showing and illustrating some Structured Streaming APIs and concepts behind issuing equivalent queries on batch as on streaming, with minimal code changes.
// MAGIC 
// MAGIC Each device entry look as follows: {"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 13, "signal": 45, "battery_level": 4, "c02_level": 1077, "timestamp" :1474652484}.
// MAGIC You can generate more data if you wish. 
// MAGIC 
// MAGIC Just follow instructions on this [github](https://github.com/dmatrix/examples/blob/master/scala/real/README.md)
// MAGIC 
// MAGIC Note: For some of you might have seen this data in this [notebook](http://dbricks.co/sswksh2)
// MAGIC 
// MAGIC To run this notebook, import it to Databricks Community Edition and attach it to a ** Runtime 3.0 Apache Spark 2.2 (Scala 2.11)** cluster.

// COMMAND ----------

// MAGIC %python
// MAGIC ACCESSY_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
// MAGIC SECERET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
// MAGIC 
// MAGIC mounts_list = [
// MAGIC {'bucket':'databricks-corp-training/structured_streaming/devices', 'mount_folder':'/mnt/sdevices'}
// MAGIC ]
// MAGIC 
// MAGIC for mount_point in mounts_list:
// MAGIC   bucket = mount_point['bucket']
// MAGIC   mount_folder = mount_point['mount_folder']
// MAGIC   try:
// MAGIC     dbutils.fs.ls(mount_folder)
// MAGIC     dbutils.fs.unmount(mount_folder)
// MAGIC   except:
// MAGIC     pass
// MAGIC   finally: #If MOUNT_FOLDER does not exist
// MAGIC     dbutils.fs.mount("s3a://"+ ACCESSY_KEY_ID + ":" + SECERET_ACCESS_KEY + "@" + bucket,mount_folder)

// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

// MAGIC %fs ls /mnt/sdevices

// COMMAND ----------

// MAGIC %fs head /mnt/sdevices/devices-1.json

// COMMAND ----------

// MAGIC %md Define a schema for the JSON Device data so that Spark doesn't have to infer it. Normally, a good idea if you have a large dataset. 

// COMMAND ----------

// import types for the scheme
import org.apache.spark.sql.types._

//fetch the JSON device information uploaded into the Filestore
val jsonFile = "dbfs:/mnt/sdevices/"
val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_type", StringType)
        .add("signal", LongType)
        .add("ip", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

// COMMAND ----------

// MAGIC %md To minimize creating many shuffle partitions and work under the resource constrains of local mode in the Community Edition, we will mininize the number of partitions. 

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

// COMMAND ----------

// MAGIC %md Read the json files from the mounted directory using the specified schema. Providing the schema avoids Spark to infer Schema, hence making the read operatoin faster. Normally, for large files, it's advisable supply the schema during read operation rather than have Spark infer.

// COMMAND ----------

val devicesStaticDF = spark
                  .read
                  .schema(jsonSchema)
                  .json(jsonFile)

// COMMAND ----------

// MAGIC %md Let's cache it and trigger an action by invoking _count()_

// COMMAND ----------

// cache the static devices
devicesStaticDF.cache()
// count the devices
devicesStaticDF.count()

// COMMAND ----------

display(devicesStaticDF)

// COMMAND ----------

// MAGIC %md ## Batch Processing 
// MAGIC 
// MAGIC Let's do a simple Batch query by counting all devices of a particular type . This will process all the devices at once or in one 
// MAGIC batch.

// COMMAND ----------

// MAGIC %md ####Q1: Can you count unique devices types whose signal is less than 5?
// MAGIC 
// MAGIC These devices probably have a weak battery, so let's find  out.

// COMMAND ----------

// import some SQL aggregate and windowing function
import org.apache.spark.sql.functions._

val staticCountsDF = devicesStaticDF
    .select("device_type", "battery_level")
    .where ("signal <= 5")
    .groupBy($"device_type", $"battery_level")
    .count()

// COMMAND ----------

// Let's register the DataFrame as table 'static_device_counts' to which we can issue Spark SQL queries
staticCountsDF.createOrReplaceTempView("static_device_counts")

// COMMAND ----------

display(staticCountsDF)

// COMMAND ----------

// MAGIC %md An equivalent SQL query on the same table. Note the above DataFrame code looks very similar to SQL

// COMMAND ----------

// MAGIC %sql select device_type, sum(count) as total_count, battery_level from static_device_counts group by device_type, battery_level

// COMMAND ----------

// MAGIC %md Effectively, what we have done here is issue three two different queries, one using high-level DataFrame APIs, and the other using SQL. Both retured the same results.

// COMMAND ----------

// MAGIC %md ## Stream Processing

// COMMAND ----------

// MAGIC %md Read the same json files from our *source*, SD directory, which contains files and each file with 100 device entries.

// COMMAND ----------

import org.apache.spark.sql.functions._

// Similar to definition of devicesStaticDF above, just using `readStream` instead of `read`
val streamingDevicesDF = spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)              // Set the schema of the JSON data, so Spark doesn't have to infer it
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json(jsonFile)

// COMMAND ----------

// MAGIC %md ####Q1: Can we do a count of all devices and use one of the output modes: complete. 

// COMMAND ----------

val deviceCountsDF = streamingDevicesDF
                     .groupBy("device_type")
                     .count()  //this count is not an action!

// COMMAND ----------

display(deviceCountsDF)

// COMMAND ----------

//lets get a query handle 
val deviceTypeQuery = deviceCountsDF
                        .writeStream
                        .queryName("device_types")
                        .format("memory")
                        .outputMode("complete")
                        .start()

// COMMAND ----------

// MAGIC %md You can use the query handle to check its status and issue other [API calls](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#monitoring-streaming-queries).

// COMMAND ----------

deviceTypeQuery.status

// COMMAND ----------

// MAGIC %sql SELECT * FROM device_types

// COMMAND ----------

// MAGIC %md The output when the stream is exhausted or the files are read will be identical to the batch or static input above

// COMMAND ----------

// MAGIC %md ### Writing to Parquet files and reading from it as a stream

// COMMAND ----------

// MAGIC %md #### Q2: Can you save it to a Parquet file, read back into a DataFrame, and query or read the parquet as data is updated?

// COMMAND ----------

// MAGIC %md ## Write to Parquet

// COMMAND ----------

val parquetQuery = streamingDevicesDF
                .writeStream
                .format("parquet")
                .option("checkpointLocation", "/tmp/checkpoint") 
                .start("/tmp/deviceParquetTable")

// COMMAND ----------

// MAGIC  %md ## Read from Parquet

// COMMAND ----------

display(spark.readStream
                .schema(jsonSchema)
                .format("parquet")
                .load("/tmp/deviceParquetTable")
                .groupBy("device_type")
                .count())

// COMMAND ----------

// MAGIC %md ####Q3: Can you issue the same query computation similar to the **BATCH QUERY in Q1** above for computing low signals but this time using the stream?

// COMMAND ----------

// Same query as staticSignalCountsDF
val streamingSignalsCountsDF = streamingDevicesDF
    .select("device_type", "battery_level")
    .where ("signal <= 5")
    .groupBy($"device_type", $"battery_level")
    .count()
// Is this DF actually a streaming DF?
streamingSignalsCountsDF.isStreaming

// COMMAND ----------

display(streamingSignalsCountsDF)

// COMMAND ----------

// MAGIC %md ### Windowing Operations using Structured Streaming APIs

// COMMAND ----------

// MAGIC %md Let's find out what sort of devices are coming in during a time window. 
// MAGIC 
// MAGIC You can change this window and play around with it—and observe how data is being updated in realtime.

// COMMAND ----------

// Same query as staticCountsDF
val streamingCountsDF = 
  streamingDevicesDF
    .groupBy($"device_type", window($"timestamp", "5 minutes"))
    .count()
    .select($"device_type", date_format($"window.end", "MMM-dd HH:mm").as("time"), $"count")
    .orderBy("time", "device_type")

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

// COMMAND ----------

display(streamingCountsDF)

// COMMAND ----------

// MAGIC %md ### Continuous aggregations using Structured Streaming APIs

// COMMAND ----------

// MAGIC %md #### Q4: Can you find some trend in the average temperature and C02 Levels over period of time from your stream of devices?

// COMMAND ----------

val streamingTrendsDF = streamingDevicesDF
                        .groupBy($"device_type", $"temp", $"c02_level", window($"timestamp", "5 minutes"))
                        .agg(avg($"c02_level").as("avg_c02"), avg($"temp").as("avg_temp"), avg($"signal"))
                        .select($"device_type", date_format($"window.end", "MMM-dd HH:mm").as("time"), $"avg_c02", $"avg_temp")
                        .orderBy("time", "device_type")
                    

// COMMAND ----------

display(streamingTrendsDF)


// COMMAND ----------

// MAGIC %md ####Compute running averages of a particular device signal strength

// COMMAND ----------

val averageQuery = streamingDevicesDF
.groupBy($"device_type", window($"timestamp", "5 minutes"))
.avg("signal")
.orderBy($"device_type", date_format($"window.end", "MMM-dd HH:mm").as("time"))

// COMMAND ----------

display(averageQuery)

// COMMAND ----------

// MAGIC %md Now do query this `parquetDF` as it's being updated

// COMMAND ----------

// MAGIC %md #####CHALLENGE 1: Can you compute the count of all devices types from each country using batch mode and DataFrame APIs?
// MAGIC 
// MAGIC HINT: you can use the same dataframe created and save into another tables, except include the **cca3** field

// COMMAND ----------

// MAGIC %md #####CHALLENGE 2: Do the same as **CHALLENGE 1** but with a streaming DataFrame

// COMMAND ----------

// MAGIC %md #####CHALLENGE 3 Can you find out all countries and devices whose C02 levels reach above 1300 from a stream?

// COMMAND ----------

val streamingC02LevelsDF = streamingDevicesDF
    .select("device_type", "c02_level", "cca3")
    .where ("c02_level >= 1100")
    .groupBy($"device_type", $"cca3")
    .count()

// COMMAND ----------

display(streamingC02LevelsDF)

// COMMAND ----------

// MAGIC %md Remove some residue that streaming leaves behind

// COMMAND ----------

dbutils.fs.rm("/tmp", true)
