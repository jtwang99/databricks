# Databricks notebook source
# MAGIC %md ###[Query semi-structured data in SQL](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html#example-data)

# COMMAND ----------

# MAGIC %sql CREATE TABLE store_data AS SELECT
# MAGIC '{
# MAGIC    "store":{
# MAGIC       "fruit": [
# MAGIC         {"weight":8,"type":"apple"},
# MAGIC         {"weight":9,"type":"pear"}
# MAGIC       ],
# MAGIC       "basket":[
# MAGIC         [1,2,{"b":"y","a":"x"}],
# MAGIC         [3,4],
# MAGIC         [5,6]
# MAGIC       ],
# MAGIC       "book":[
# MAGIC         {
# MAGIC           "author":"Nigel Rees",
# MAGIC           "title":"Sayings of the Century",
# MAGIC           "category":"reference",
# MAGIC           "price":8.95
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"Herman Melville",
# MAGIC           "title":"Moby Dick",
# MAGIC           "category":"fiction",
# MAGIC           "price":8.99,
# MAGIC           "isbn":"0-553-21311-3"
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"J. R. R. Tolkien",
# MAGIC           "title":"The Lord of the Rings",
# MAGIC           "category":"fiction",
# MAGIC           "reader":[
# MAGIC             {"age":25,"name":"bob"},
# MAGIC             {"age":26,"name":"jack"}
# MAGIC           ],
# MAGIC           "price":22.99,
# MAGIC           "isbn":"0-395-19395-8"
# MAGIC         }
# MAGIC       ],
# MAGIC       "bicycle":{
# MAGIC         "price":19.95,
# MAGIC         "color":"red"
# MAGIC       }
# MAGIC     },
# MAGIC     "owner":"amy",
# MAGIC     "zip code":"94025",
# MAGIC     "fb:testid":"1234"
# MAGIC  }' as raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw:owner, RAW:owner FROM store_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- References are case sensitive when you use brackets
# MAGIC SELECT raw:OWNER case_insensitive, raw:['OWNER'] case_sensitive FROM store_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use backticks to escape special characters. References are case insensitive when you use backticks.
# MAGIC -- Use brackets to make them case sensitive.
# MAGIC SELECT raw:`zip code`, raw:`Zip Code`, raw:['fb:testid'] FROM store_data

# COMMAND ----------

# MAGIC %md ###[Extract nested fields](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html#extract-nested-fields)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use dot notation
# MAGIC SELECT raw:store.bicycle FROM store_data
# MAGIC -- the column returned is a string

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use brackets
# MAGIC SELECT raw:store['bicycle'], raw:store['BICYCLE'] FROM store_data

# COMMAND ----------

# MAGIC %sql SELECT
# MAGIC     raw:store.basket[*],
# MAGIC     raw:store.basket[*][0] first_of_baskets,
# MAGIC     raw:store.basket[0][*] first_basket,
# MAGIC     raw:store.basket[*][*] all_elements_flattened,
# MAGIC     raw:store.basket[0][2].b subfield
# MAGIC FROM store_data

# COMMAND ----------

# MAGIC %sql SELECT raw:store.bicycle.price::double FROM store_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use from_json to cast into more complex types
# MAGIC SELECT from_json(raw:store.bicycle, 'price double, color string') bicycle FROM store_data
# MAGIC -- the column returned is a struct containing the columns price and color

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_json(raw:store.basket[*], 'array<array<string>>') baskets FROM store_data
# MAGIC -- the column returned is an array of string arrays

# COMMAND ----------

# MAGIC %sql select '{"key":null}':key is null sql_null, '{"key":null}':key == 'null' text_null

# COMMAND ----------


