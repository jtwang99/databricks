# Databricks notebook source
# MAGIC %md ### ![Spark Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark.png) + ![SF Open Data Logo](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/logo_sfopendata.png)

# COMMAND ----------

# MAGIC %md Mount the data:

# COMMAND ----------

ACCESSY_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
SECERET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF" 

mounts_list = [
{'bucket':'databricks-corp-training/sf_open_data/', 'mount_folder':'/mnt/sf_open_data'}
]

# COMMAND ----------

for mount_point in mounts_list:
  bucket = mount_point['bucket']
  mount_folder = mount_point['mount_folder']
  try:
    dbutils.fs.ls(mount_folder)
    dbutils.fs.unmount(mount_folder)
  except:
    pass
  finally: #If MOUNT_FOLDER does not exist
    dbutils.fs.mount("s3a://"+ ACCESSY_KEY_ID + ":" + SECERET_ACCESS_KEY + "@" + bucket,mount_folder)

# COMMAND ----------

# MAGIC %md ## Exploring the City of San Francisco public data with Apache Spark 2.0

# COMMAND ----------

# MAGIC %md It was the 4th of July a couple of weeks ago, so SF residents enjoyed a fireworks show:

# COMMAND ----------

# MAGIC %md ![Fireworks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/fireworks.png)

# COMMAND ----------

# MAGIC %md How did the 4th of July holiday affect demand for Firefighters a year ago in SF districts?

# COMMAND ----------

# MAGIC %md The SF OpenData project was launched in 2009 and contains hundreds of datasets from the city and county of San Francisco. Open government data has the potential to increase the quality of life for residents, create more efficient government services, better public decisions, and even new local businesses and services.
# MAGIC 
# MAGIC In our analysis of SF Fire Department calls, we will be seeking answers the following questions:
# MAGIC 
# MAGIC 1. **How many different types of calls were made to the Fire Department?**
# MAGIC 2. **How many incidents of each call type were there?**
# MAGIC 3. **How many incidents of each call type were there?**
# MAGIC 4. **How many service calls were logged in the past 7 days?**
# MAGIC 5. **Which neighborhood in SF generated the most calls last year?**
# MAGIC 6. **What was the primary non-medical reason most people called the fire department from the Tenderloin last year?**

# COMMAND ----------

# MAGIC %md ## Introduction to Spark

# COMMAND ----------

# MAGIC %md Spark is a unified processing engine that can analyze big data using SQL, machine learning, graph processing or real time stream analysis:
# MAGIC 
# MAGIC ![Spark Engines](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_4engines.png)
# MAGIC 
# MAGIC We will mostly focus on Spark SQL and DataFrames this evening.

# COMMAND ----------

# MAGIC %md Spark can read from many different databases and file systems and run in various environments:
# MAGIC 
# MAGIC ![Spark Goal](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_goal.png)

# COMMAND ----------

# MAGIC %md Although Spark supports four languages (Scala, Java, Python, R), tonight we will use Python.
# MAGIC Broadly speaking, there are **2 APIs** for interacting with Spark:
# MAGIC - **DataFrames/SQL/Datasets:** general, higher level API for users of Spark
# MAGIC - **RDD:** a lower level API for spark internals and advanced programming

# COMMAND ----------

# MAGIC %md A Spark cluster is made of one Driver and many Executor JVMs (java virtual machines):

# COMMAND ----------

# MAGIC %md ![Spark Physical Cluster, slots](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_slots.png)

# COMMAND ----------

# MAGIC %md The Driver sends Tasks to the empty slots on the Executors when work has to be done:

# COMMAND ----------

# MAGIC %md ![Spark Physical Cluster, tasks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/spark_cluster_tasks.png)

# COMMAND ----------

# MAGIC %md In Databricks Community Edition, everyone gets a local mode cluster, where the Driver and Executor code run in the same JVM. Local mode clusters are typically used for prototyping and learning Spark:

# COMMAND ----------

# MAGIC %md ![Notebook + Micro Cluster](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/notebook_microcluster.png)

# COMMAND ----------

# MAGIC %md ![Databricks](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/book_intro/databricks_about.png)

# COMMAND ----------

# MAGIC %md ## Introduction to Fire Department Calls for Service

# COMMAND ----------

# MAGIC %md The latest July 6th, 2016 copy of the "Fire Department Calls for Service" data set has been uploaded to S3. You can see the data with the `%fs ls` command:

# COMMAND ----------

# MAGIC %fs ls /mnt/sf_open_data/fire_dept_calls_for_service/

# COMMAND ----------

# MAGIC %md Note, you can also access the 1.6 GB of data directly from sfgov.org via this link: https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3

# COMMAND ----------

# MAGIC %md The entry point into all functionality in Spark 2.x is the new SparkSession class, which we explored in our previous workshop: http://dbricks.co/ss_wkshp1

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md Using the SparkSession, create a DataFrame from the CSV file by inferring the schema:

# COMMAND ----------

fireServiceCallsDF = spark.read.csv('/mnt/sf_open_data/fire_dept_calls_for_service/Fire_Department_Calls_for_Service.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md Notice that the above cell takes ~15 seconds to run because it is inferring the schema by sampling the file and reading through it.
# MAGIC 
# MAGIC Inferring the schema works for ad hoc analysis against smaller datasets. But when working on multi-TB+ data, it's better to provide an **explicit pre-defined schema manually**, so there's no inferring cost:

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# COMMAND ----------

# Note that we are removing all space characters from the col names to prevent errors when writing to Parquet later

fireSchema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),       
                     StructField('WatchDate', StringType(), True),       
                     StructField('ReceivedDtTm', StringType(), True),       
                     StructField('EntryDtTm', StringType(), True),       
                     StructField('DispatchDtTm', StringType(), True),       
                     StructField('ResponseDtTm', StringType(), True),       
                     StructField('OnSceneDtTm', StringType(), True),       
                     StructField('TransportDtTm', StringType(), True),                  
                     StructField('HospitalDtTm', StringType(), True),       
                     StructField('CallFinalDisposition', StringType(), True),       
                     StructField('AvailableDtTm', StringType(), True),       
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('ZipcodeofIncident', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumberofAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('Unitsequenceincalldispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('NeighborhoodDistrict', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True)])

# COMMAND ----------

#Notice that no job is run this time
# Python is not typed language so Datasets don't exists. Instead, we always get back a DataFrame
fireServiceCallsDF = spark.read.csv('/mnt/sf_open_data/fire_dept_calls_for_service/Fire_Department_Calls_for_Service.csv', header=True, schema=fireSchema)

# COMMAND ----------

# MAGIC %md Look at the first 5 records in the DataFrame:

# COMMAND ----------

display(fireServiceCallsDF.limit(5))

# COMMAND ----------

# MAGIC %md Let's examine the schema

# COMMAND ----------

fireServiceCallsDF.printSchema()

# COMMAND ----------

# MAGIC %md Print just the column names in the DataFrame:

# COMMAND ----------

fireServiceCallsDF.columns

# COMMAND ----------

# MAGIC %md Count how many rows total there are in DataFrame (and see how long it takes to do a full scan from remote disk/S3):

# COMMAND ----------

fireServiceCallsDF.count()

# COMMAND ----------

# MAGIC %md There are over 4 million rows in the DataFrame and it takes ~14 seconds to do a full read of it.

# COMMAND ----------

# MAGIC %md Open the Apache Spark 2.0 release documentation in new tabs, so you can easily reference the API guide:
# MAGIC 
# MAGIC 1) Spark 2.0 docs: http://spark.apache.org/docs/latest/api/python/index.html
# MAGIC 
# MAGIC 2) DataFrame user documentation: http://spark.apache.org/docs/latest/sql-programming-guide.html
# MAGIC 
# MAGIC 3) PySpark API 2.0 docs: http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **Analysis with PySpark DataFrames API**

# COMMAND ----------

# MAGIC %md ####![Spark Operations](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/spark_ta.png)

# COMMAND ----------

# MAGIC %md DataFrames support two types of operations: *transformations* and *actions*.
# MAGIC 
# MAGIC Transformations, like `select()` or `filter()` create a new DataFrame from an existing one.
# MAGIC 
# MAGIC Actions, like `show()` or `count()`, return a value with results to the user. Other actions like `save()` write the DataFrame to distributed storage (like S3 or HDFS).

# COMMAND ----------

# MAGIC %md ####![Spark T/A](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/pagecounts/trans_and_actions.png)

# COMMAND ----------

# MAGIC %md Transformations contribute to a query plan,  but  nothing is executed until an action is called.

# COMMAND ----------

# MAGIC %md **Q-1) How many different types of calls were made to the Fire Department?**

# COMMAND ----------

# Use the .select() transformation to yank out just the 'Call Type' column, then call the show action
# show is OSS version of display(), which exists only in the Databricks notebooks. show() gives you the look and feel of REPL,
# the shell on the commandline for pyspark
# Notice the syntax: we reference the column by name
fireServiceCallsDF.select('CallType').show(5)

# COMMAND ----------

# Add the .distinct() transformation to keep only distinct rows
# The False below expands the ASCII column width to fit the full text in the output

fireServiceCallsDF.select('CallType').distinct().show(35, False)

# COMMAND ----------

# MAGIC %md **Q-2) How many incidents of each call type were there?**

# COMMAND ----------

#Note that .count() is actually a transformation here

display(fireServiceCallsDF.select('CallType').groupBy('CallType').count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md Seems like the SF Fire department is called for _medical incidents_ far more than any other type. Note that the above command took about 14 seconds to execute. In an upcoming section, we'll cache the data into memory for up to 100x speed increases.

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** Doing ETL and Date/Time Analysis**

# COMMAND ----------

# MAGIC %md **Q-3) How many years of Fire Service Calls is in the data file?**

# COMMAND ----------

# MAGIC %md Notice that the date or time columns are currently being interpreted as strings, rather than date or time objects:

# COMMAND ----------

fireServiceCallsDF.printSchema()

# COMMAND ----------

# MAGIC %md Let's use the unix_timestamp() function to convert the string into a timestamp:
# MAGIC 
# MAGIC http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.functions.from_unixtime

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Note that PySpark uses the Java Simple Date Format patterns

from_pattern1 = 'MM/dd/yyyy'
to_pattern1 = 'yyyy-MM-dd'

from_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'
to_pattern2 = 'MM/dd/yyyy hh:mm:ss aa'


fireServiceCallsTsDF = fireServiceCallsDF \
  .withColumn('CallDateTS', unix_timestamp(fireServiceCallsDF['CallDate'], from_pattern1).cast("timestamp")) \
  .drop('CallDate') \
  .withColumn('WatchDateTS', unix_timestamp(fireServiceCallsDF['WatchDate'], from_pattern1).cast("timestamp")) \
  .drop('WatchDate') \
  .withColumn('ReceivedDtTmTS', unix_timestamp(fireServiceCallsDF['ReceivedDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ReceivedDtTm') \
  .withColumn('EntryDtTmTS', unix_timestamp(fireServiceCallsDF['EntryDtTm'], from_pattern2).cast("timestamp")) \
  .drop('EntryDtTm') \
  .withColumn('DispatchDtTmTS', unix_timestamp(fireServiceCallsDF['DispatchDtTm'], from_pattern2).cast("timestamp")) \
  .drop('DispatchDtTm') \
  .withColumn('ResponseDtTmTS', unix_timestamp(fireServiceCallsDF['ResponseDtTm'], from_pattern2).cast("timestamp")) \
  .drop('ResponseDtTm') \
  .withColumn('OnSceneDtTmTS', unix_timestamp(fireServiceCallsDF['OnSceneDtTm'], from_pattern2).cast("timestamp")) \
  .drop('OnSceneDtTm') \
  .withColumn('TransportDtTmTS', unix_timestamp(fireServiceCallsDF['TransportDtTm'], from_pattern2).cast("timestamp")) \
  .drop('TransportDtTm') \
  .withColumn('HospitalDtTmTS', unix_timestamp(fireServiceCallsDF['HospitalDtTm'], from_pattern2).cast("timestamp")) \
  .drop('HospitalDtTm') \
  .withColumn('AvailableDtTmTS', unix_timestamp(fireServiceCallsDF['AvailableDtTm'], from_pattern2).cast("timestamp")) \
  .drop('AvailableDtTm')  

# COMMAND ----------

fireServiceCallsTsDF.printSchema()

# COMMAND ----------

# MAGIC %md Notice that the formatting of the timestamps is now different:

# COMMAND ----------

display(fireServiceCallsTsDF.limit(5))

# COMMAND ----------

# MAGIC %md Finally calculate how many distinct years of data is in the CSV file: Use the sql utility function _year()_: http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.functions.year

# COMMAND ----------

fireServiceCallsTsDF.select(year('CallDateTS')).distinct().orderBy('year(CallDateTS)').show()

# COMMAND ----------

# MAGIC %md **CHALLENGE: Can you show the years in descending order?**

# COMMAND ----------

fireServiceCallsTsDF.select(year('CallDateTS')).distinct().orderBy('year(CallDateTS)', ascending=False).show()

# COMMAND ----------

# MAGIC %md **Q-4) How many service calls were logged in the past 7 days?**

# COMMAND ----------

# MAGIC %md Note that today, July 6th, is the 187th day of the year.
# MAGIC 
# MAGIC Filter the DF down to just 2016 and days of year greater than 180:
# MAGIC Use the following SQL utility funcitons:
# MAGIC * _year()_
# MAGIC * _dayofyear()_

# COMMAND ----------

fireServiceCallsTsDF.filter(year('CallDateTS') == '2016') \
  .filter(dayofyear('CallDateTS') >= 180) \
  .select(dayofyear('CallDateTS')) \
  .distinct().orderBy('dayofyear(CallDateTS)') \
  .show()

# COMMAND ----------

# MAGIC %md What if we want to find out the number of calls on each day of the last 7 days?

# COMMAND ----------

fireServiceCallsTsDF.filter(year('CallDateTS') == '2016') \
      .filter(dayofyear('CallDateTS') >= 180) \
      .groupBy(dayofyear('CallDateTS')) \
      .count() \
      .orderBy('dayofyear(CallDateTS)') \
      .show()

# COMMAND ----------

# MAGIC %md Note above that July 4th, 2016 was the 185th day of the year.

# COMMAND ----------

# MAGIC %md Visualize the results in a bar graph:

# COMMAND ----------

display(fireServiceCallsTsDF.filter(year('CallDateTS') == '2016').filter(dayofyear('CallDateTS') >= 180).groupBy(dayofyear('CallDateTS')).count().orderBy('dayofyear(CallDateTS)'))

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** Memory, Caching and Write to Parquet**

# COMMAND ----------

# MAGIC %md The DataFrame is currently comprised of 13 partitions:

# COMMAND ----------

fireServiceCallsTsDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md ![Partitions](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/df_13_parts.png)

# COMMAND ----------

fireServiceCallsTsDF.repartition(6).createOrReplaceTempView("fireServiceVIEW");

# COMMAND ----------

spark.catalog.cacheTable("fireServiceVIEW")

# COMMAND ----------

# Call .count() to materialize the cache
spark.table("fireServiceVIEW").count()

# COMMAND ----------

fireServiceDF = spark.table("fireServiceVIEW")

# COMMAND ----------

# Note that the full scan + count in memory takes < 1 second!

fireServiceDF.count()

# COMMAND ----------

spark.catalog.isCached("fireServiceVIEW")

# COMMAND ----------

# MAGIC %md The 6 partitions are now cached in memory:

# COMMAND ----------

# MAGIC %md ![6 Partitions](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/df_6_parts.png)

# COMMAND ----------

# MAGIC %md Use the Spark UI to see the 6 partitions in memory:

# COMMAND ----------

# MAGIC %md ![Mem UI](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/mem_ui.png)

# COMMAND ----------

# MAGIC %md Now that our data has the correct date types for each column and it is correctly partitioned, let's write it down as a parquet file for future loading:

# COMMAND ----------

# MAGIC %fs ls /tmp/

# COMMAND ----------

fireServiceDF.write.format('parquet').save('/tmp/fireServiceParquet/')

# COMMAND ----------

# MAGIC %md Now the directory should contain 6 .gz compressed Parquet files (one for each partition):

# COMMAND ----------

# MAGIC %fs ls /tmp/fireServiceParquet/

# COMMAND ----------

# MAGIC %md Here's how you can easily read the parquet file from S3 in the future:

# COMMAND ----------

tempDF = spark.read.parquet('/tmp/fireServiceParquet/')

# COMMAND ----------

display(tempDF.limit(2))

# COMMAND ----------

# MAGIC %md Did you know that the new vectorized Parquet decoder in Spark 2.0 has improved Parquet scan throughput by 3x?

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) **SQL Queries**

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM fireServiceVIEW;

# COMMAND ----------

# MAGIC %md You can use the Spark Stages UI to see the 6 tasks launched in the middle stage:

# COMMAND ----------

# MAGIC %md ![Event Timeline](http://curriculum-release.s3-website-us-west-2.amazonaws.com/sf_open_data_meetup/event_timeline.png)

# COMMAND ----------

# MAGIC %md **Q-5) Which neighborhood in SF generated the most calls in year 2015?**

# COMMAND ----------

# MAGIC %sql SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM fireServiceVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15;

# COMMAND ----------

# MAGIC %md Expand the Spark Job details in the cell above and notice that the last stage uses 200 partitions! This is default is non-optimal, given that we only have ~1.6 GB of data and 3 slots.
# MAGIC 
# MAGIC Change the shuffle.partitions option to 6:

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 6)

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md Re-run the same SQL query and notice the speed increase:

# COMMAND ----------

# MAGIC %sql SELECT `NeighborhoodDistrict`, count(`NeighborhoodDistrict`) AS Neighborhood_Count FROM fireServiceVIEW WHERE year(`CallDateTS`) == '2015' GROUP BY `NeighborhoodDistrict` ORDER BY Neighborhood_Count DESC LIMIT 15;

# COMMAND ----------

# MAGIC %md SQL also has some handy commands like `DESC` (describe) to see the schema + data types for the table:

# COMMAND ----------

# MAGIC %sql DESC fireServiceVIEW;

# COMMAND ----------

# MAGIC %md ### ![Spark Logo Tiny](http://curriculum-release.s3-website-us-west-2.amazonaws.com/wiki-book/general/logo_spark_tiny.png) ** DataFrame Joins**

# COMMAND ----------

# MAGIC %md **Q-6) What was the primary non-medical reason most people called the fire department from the Tenderloin last year?**

# COMMAND ----------

# MAGIC %md The "Fire Incidents" data includes a summary of each (non-medical) incident to which the SF Fire Department responded.

# COMMAND ----------

# MAGIC %md Let's do a join to the Fire Incidents data on the "Incident Number" column:
# MAGIC 
# MAGIC https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric

# COMMAND ----------

# MAGIC %md Read the Fire Incidents CSV file into a DataFrame:

# COMMAND ----------

incidentsDF = spark.read.csv('/mnt/sf_open_data/fire_incidents/Fire_Incidents.csv', header=True, inferSchema=True).withColumnRenamed('Incident Number', 'IncidentNumber').cache()

# COMMAND ----------

incidentsDF.printSchema()

# COMMAND ----------

# Materialize the cache
incidentsDF.count()

# COMMAND ----------



# COMMAND ----------

display(incidentsDF.limit(3))

# COMMAND ----------

# MAGIC %md Do an [inner join](https://www.w3schools.com/sql/sql_join_inner.asp) on the column IncidentNumber between two DataFrames. 
# MAGIC The inner `join()` DataFrame selects rows that have matching values in both DataFrames. In our case, we will include all columns
# MAGIC from DataFrames fireServiceDF and incidentsDF where their column `incidentNumber` match.
# MAGIC 
# MAGIC Surely, this will result into a wider DataFrame but fewer rows.

# COMMAND ----------

joinedDF = fireServiceDF.join(incidentsDF, fireServiceDF.IncidentNumber == incidentsDF.IncidentNumber)

# COMMAND ----------

joinedDF.columns

# COMMAND ----------

display(joinedDF.limit(3))

# COMMAND ----------

#Note that the joined DF is only 1.1 million rows because we did an inner join (the original Fire Service Calls data had 4+ million rows)
joinedDF.count()

# COMMAND ----------

# MAGIC %md What's the count of Tenderloin districts in the joined DataFrame?

# COMMAND ----------

joinedDF.filter(year('CallDateTS') == '2015') \
      .filter(col('NeighborhoodDistrict') == 'Tenderloin') \
      .count()

# COMMAND ----------

# MAGIC %md Now the last bit where where we ascertain what was the `Primary Situation` that resulted into the Fire Service Call from the Tenderloin District?

# COMMAND ----------

display(joinedDF.filter(year('CallDateTS') == '2015') \
        .filter(col('NeighborhoodDistrict') == 'Tenderloin') \
        .groupBy('Primary Situation') \
        .count() \
        .orderBy(desc("count")) \
        .limit(10))

# COMMAND ----------

# MAGIC %md Most of the calls were False Alarms!

# COMMAND ----------

# MAGIC %md What do residents of Russian Hill call the fire department for?

# COMMAND ----------

display(joinedDF.filter(year('CallDateTS') == '2015') \
        .filter(col('NeighborhoodDistrict') == 'Russian Hill') \
        .groupBy('Primary Situation') \
        .count() \
        .orderBy(desc("count")) \
        .limit(10))

# COMMAND ----------

# MAGIC %md #### UBER CHALLENGE: Can you convert this into Scala code and use Datasets?

# COMMAND ----------

# MAGIC %md ### ** Keep Hacking! **
