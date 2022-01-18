// Databricks notebook source
// MAGIC %md This example notebook shows you how to flatten nested JSON, using only `$"column.*"` and `explode` methods.
// MAGIC 
// MAGIC Start by passing the sample JSON string to the reader.

// COMMAND ----------

val json ="""
{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}
"""

// COMMAND ----------

// MAGIC %md Add the JSON string as a collection type and pass it as an input to `spark.createDataset`. This converts it to a DataFrame. The JSON reader infers the schema automatically from the JSON string.
// MAGIC 
// MAGIC This sample code uses a list collection type, which is represented as `json :: Nil`. You can also use other Scala collection types, such as Seq (Scala Sequence).

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
val DF= spark.read.json(spark.createDataset(json :: Nil))

// COMMAND ----------

// MAGIC %md Display the DataFrame to view the current state.

// COMMAND ----------

display(DF)

// COMMAND ----------

// MAGIC %md Use `$"column.*"` and `explode` methods to flatten the struct and array types before displaying the flattened DataFrame.

// COMMAND ----------

display(DF.select($"id" as "main_id",$"name",$"batters",$"ppu",explode($"topping")) // Exploding the topping column using explode as it is an array type
        .withColumn("topping_id",$"col.id") // Extracting topping_id from col using DOT form
        .withColumn("topping_type",$"col.type") // Extracting topping_tytpe from col using DOT form
        .drop($"col")
        .select($"*",$"batters.*") // Flattened the struct type batters tto array type which is batter
        .drop($"batters")
        .select($"*",explode($"batter"))
        .drop($"batter")
        .withColumn("batter_id",$"col.id") // Extracting batter_id from col using DOT form
        .withColumn("battter_type",$"col.type") // Extracting battter_type from col using DOT form
        .drop($"col")
       )

// COMMAND ----------


