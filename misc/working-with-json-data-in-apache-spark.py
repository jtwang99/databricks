# Databricks notebook source
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data

# COMMAND ----------

# MAGIC %md
# MAGIC {
# MAGIC 	"id": "0001",
# MAGIC 	"type": "donut",
# MAGIC 	"name": "Cake",
# MAGIC 	"ppu": 0.55,
# MAGIC 	"batters":
# MAGIC 		{
# MAGIC 			"batter":
# MAGIC 				[
# MAGIC 					{ "id": "1001", "type": "Regular" },
# MAGIC 					{ "id": "1002", "type": "Chocolate" },
# MAGIC 					{ "id": "1003", "type": "Blueberry" },
# MAGIC 					{ "id": "1004", "type": "Devil's Food" }
# MAGIC 				]
# MAGIC 		},
# MAGIC 	"topping":
# MAGIC 		[
# MAGIC 			{ "id": "5001", "type": "None" },
# MAGIC 			{ "id": "5002", "type": "Glazed" },
# MAGIC 			{ "id": "5005", "type": "Sugar" },
# MAGIC 			{ "id": "5007", "type": "Powdered Sugar" },
# MAGIC 			{ "id": "5006", "type": "Chocolate with Sprinkles" },
# MAGIC 			{ "id": "5003", "type": "Chocolate" },
# MAGIC 			{ "id": "5004", "type": "Maple" }
# MAGIC 		]
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Sample JSON File

# COMMAND ----------

rawDF = spark.read.json("../data/sample.json", multiLine = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore DataFrame Schema

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Convert "batters" Nested Structure to Simple DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename ID to Key

# COMMAND ----------

sampleDF = rawDF.withColumnRenamed("id", "key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select "batters" columns

# COMMAND ----------

batDF = sampleDF.select("key", "batters.batter")
batDF.printSchema()

# COMMAND ----------

batDF.show(1, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a row for each element : explode

# COMMAND ----------

bat2DF = batDF.select("key", explode("batter").alias("new_batter"))
bat2DF.show()

# COMMAND ----------

bat2DF.printSchema()

# COMMAND ----------

bat2DF.select("key", "new_batter.*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a row for each struct element : explode

# COMMAND ----------

finalBatDF = (sampleDF
        .select("key", explode("batters.batter").alias("new_batter"))
        .select("key", "new_batter.*")
        .withColumnRenamed("id", "bat_id")
        .withColumnRenamed("type", "bat_type"))
finalBatDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select element from Array : topping

# COMMAND ----------

sampleDF.select(col("topping").getItem(0).id.alias("top_id"), col("topping").getItem(0).type.alias("top_type")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Convert "toppings" nested structure to simple DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a row for each Array element : explode

# COMMAND ----------

topDF = (sampleDF
        .select("key", explode("topping").alias("new_topping"))
        .select("key", "new_topping.*")
        .withColumnRenamed("id", "top_id")
        .withColumnRenamed("type", "top_type")
        )
topDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Thank You
