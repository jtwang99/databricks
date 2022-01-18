# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Dive in Apache Spark DateTime functions

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook code supports the blog:  
# MAGIC #### Link to be added.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Reuired Libraries

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create SparkSession and SparkContext

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample DataFrame

# COMMAND ----------

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### add_months

# COMMAND ----------

# Add the months to the date. It will return new date after the month from the start date.
# For e.g. in below statement we have added 1 months to the column "date" and generated new column as "next_month"
df = (empdf
    .select("date")
    .withColumn("next_month", add_months("date", 1)))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### current_date

# COMMAND ----------

# It will return current date.
df = (empdf
    .withColumn("current_date", current_date())
    .select("id", "current_date"))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### current_timestamp

# COMMAND ----------

# It will return current timestamp.
df = (empdf
    .withColumn("current_timestamp", current_timestamp())
    .select("id", "current_timestamp"))
df.show(2,False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_add

# COMMAND ----------

# It will gives the date days after the start date mentioned in the function.
# for example below statement will return the date after say 5 days in new column as "next_date"
df = (empdf
    .select("date")
    .withColumn("next_date", date_add("date", 5)))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_format

# COMMAND ----------

# Convert the date to specified format.
# For e.g. we will convert the date from "yyyy-MM-dd" to "dd/MM/yyyy" format.
df = (empdf
      .select("date")
      .withColumn("date_string", date_format("date", "yyyy/MM/dd hh:mm:ss")))
df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_sub

# COMMAND ----------

# It will return the days before the start date. Opposite of date_add.
# for example below statement will return the date before say 5 days in new column as "new_date"
df = (empdf
    .select("date")
    .withColumn("new_date", date_sub("date", 5)))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_trunc : based on year

# COMMAND ----------

# It will return timestamp truncated to the specified unit.
# Lets truncate date by year. we can use "yyyy" or "yy" or" "year" to specify Year.
df = (empdf
    .select("date")
    .withColumn("new_date", date_trunc("year", "date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_trunc : based on Month

# COMMAND ----------

# Lets truncate date by Month. we can use "mm" or "month" or" "mon" to specify Month.
df = (empdf
    .select("date")
    .withColumn("new_date", date_trunc("month", "date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_trunc : based on Day

# COMMAND ----------

# Lets truncate date by Day. we can use "day" or "dd"  to specify Day.
df = (empdf
    .select("date")
    .withColumn("new_date", date_trunc("day", "date")))
df.show(2)

# Note can use these many formats to truncate the date based on different level.
# Format : ‘year’, ‘yyyy’, ‘yy’, ‘month’, ‘mon’, ‘mm’, ‘day’, ‘dd’, ‘hour’, ‘minute’, ‘second’, ‘week’, ‘quarter’

# COMMAND ----------

# MAGIC %md
# MAGIC ### datediff

# COMMAND ----------

# It will return the difference between the dates in terms of days
# Lets add another column as current date and takes the difference with "date" column here.
df = (empdf.select("date")
        # Add another date column as current date.
        .withColumn("current_date", current_date()) 
        # Take the difference between current_date and date column.
        .withColumn("date_diff", datediff("current_date", "date"))) 
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dayofmonth

# COMMAND ----------

# It will return the date of the month.
# For e.g. for 5th Jan 2019 (2019-01-05) it will return 5. 
df = (empdf
    .select("date")
    .withColumn("dayofmonth", dayofmonth("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dayofweek

# COMMAND ----------

# it will return day of week as integer. It will consider Sunday as 1st day and Saturday as 7th Day.
df = (empdf
    .select("date")
    .withColumn("dayofweek", dayofweek("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dayofyear

# COMMAND ----------

# It will return day of the year as integer.
# For e.g. for 5th Jan it will return 5. for 1st Feb it will return 32.
df = (empdf
    .select("date")
    .withColumn("dayofyear", dayofyear("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### from_utc_timestamp

# COMMAND ----------

# Convert UTC timestamp to timestamp of any specified timezone. Default, it will assume that date is in UTC timestamp.
# For e.g. lets convert the UTC timestamp to "PST" time.
df = (empdf
    .select("date")
    .withColumn("pst_timestamp", from_utc_timestamp("date", "PST")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### unix_timestamp

# COMMAND ----------

# Convert timestamp string with given format to Unix time stamp (in Seconds). Default format is "yyyy-MM-dd HH:mm:ss".
# You can use spark property : "spark.sql.session.timeZone" to set the timezone.

df = (empdf
    .select("date")
    .withColumn("unix_timestamp", unix_timestamp("date", "yyyy-MM-dd HH:mm:ss")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### from_unixtime

# COMMAND ----------

# Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a given string format. You can set the timezone and format as well.
# You can use spark property : "spark.sql.session.timeZone" to set the timezone.

df = (empdf
    .select("date")
    # Convert timestamp to unix timestamp.
    .withColumn("unix_timestamp", unix_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
    # Convert unix timestamp to timestamp.
    .withColumn("date_from_unixtime", from_unixtime("unix_timestamp")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### hour

# COMMAND ----------

# It will return hour part of the date.
df = (empdf
    .select("date")
    .withColumn("hour", hour("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### last_day

# COMMAND ----------

# It will return last of the Month for a given date.
# For e.g. for 5th Jan 2019, it will return 31st Jan 2019, since this is the last date for the Month.

df = empdf.select("date").withColumn("last_date", last_day("date"))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### minute

# COMMAND ----------

# It will return minute part of the date.
df = (empdf
    .select("date")
    .withColumn("minute", minute("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### month

# COMMAND ----------

# It will return month part of the date.
df = (empdf
    .select("date")
    .withColumn("month", month("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### months_between

# COMMAND ----------

# It will return the difference between the dates in terms of months. 
# If first date is greater than second one result will be positive else negative.
# For e.g. between 6th Feb 2019 and 5th Jan 2019 it will return 1.

df = (empdf
    .select("date")
    # Add another date column as current date.        
    .withColumn("current_date", current_date()) 
    # Take the difference between current_date and date column in terms of months.
    .withColumn("months_between", months_between("current_date", "date"))) 
df.show(2)

# Note from Spark 2.4.0 onwards you can specify third argument "roundOff=True" to round-Off the value. 
# Default value is True.

# COMMAND ----------

# MAGIC %md
# MAGIC ### next_day

# COMMAND ----------

# It will return the next day based on the dayOfWeek specified in next argument.
# For e.g. for 1st Feb 2019 (Friday) if we ask for next_day as sunday, it will return 3rd Feb 2019.

df = (empdf
    .select("date")
    .withColumn("next_day", next_day("date", "sun")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### quarter

# COMMAND ----------

# It will return quarter of the given date as integer.

df = (empdf
    .select("date")
    .withColumn("quarter", quarter("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### second

# COMMAND ----------

# It will return the second part of the date.

df = (empdf
    .select("date")
    .withColumn("second", second("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_date

# COMMAND ----------

# It will convert the String or TimeStamp to Date.

df = (empdf
    .select("date")
    .withColumn("to_date", to_date("date")))
df.show(2)

# Note : Check the data type of column "date" and "to-date".
# If the string format is 'yyyy-MM-dd HH:mm:ss' then we need not to specify the format. 
# Otherwise, specify the format as second arg in to_date function.

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_date with different date format.

# COMMAND ----------

# Here we will convert the string of format 'dd/MM/yyyy HH:mm:ss' to "date" data type. Default format it 'yyyy-MM-dd`.
df1 = spark.createDataFrame([('15/02/2019 10:30:00',)], ['date'])
df2 = (df1
    .withColumn("new_date", to_date("date", 'dd/MM/yyyy HH:mm:ss')))
    
df2.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_timestamp

# COMMAND ----------

# It will convert the String to TimeStamp. Here we will convert the string of format 'dd/MM/yyyy HH:mm:ss' to "timestamp" data type.
# default format is 'yyyy-MM-dd HH:mm:ss'

df1 = spark.createDataFrame([('15/02/2019 10:30:00',)], ['date'])
df2 = (df1
    .withColumn("new_date", to_timestamp("date", 'dd/MM/yyyy HH:mm:ss')))
df2.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### to_utc_timestamp

# COMMAND ----------

# Convert given timestamp to UTC timestamp.
# For e.g. lets convert the "PST" timestamp to "UTC" timestamp.
df = (empdf
    .select("date")
    .withColumn("utc_timestamp", to_utc_timestamp("date", "PST")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### weekofyear

# COMMAND ----------

# It will return the weekofyear for the given date.

df = (empdf
    .select("date")
    .withColumn("weekofyear", weekofyear("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### year

# COMMAND ----------

# It will return the year part of the date.

df = (empdf
    .select("date")
    .withColumn("year", year("date")))
df.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC # THANK YOU
