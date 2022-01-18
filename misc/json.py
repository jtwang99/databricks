# Databricks notebook source
# MAGIC %md ###[json_tuple](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/json_tuple)

# COMMAND ----------

# MAGIC %sql SELECT json_tuple('{"a":1, "b":2}', 'a', 'b') AS (col_a, col_b), 'Spark SQL';

# COMMAND ----------

# MAGIC %sql SELECT json_tuple('{"a":1, "b":2}', 'a', 'c'), 'Spark SQL';

# COMMAND ----------

# MAGIC %md ###[json_object_keys](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/json_object_keys)

# COMMAND ----------

# MAGIC %sql SELECT json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');

# COMMAND ----------

# MAGIC %md ###[get_json_object](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/get_json_object)

# COMMAND ----------

# MAGIC %sql SELECT json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');

# COMMAND ----------

# MAGIC %md ###[from_json](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/from_json)

# COMMAND ----------

# MAGIC %sql SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));

# COMMAND ----------

# MAGIC %md ###[json_array_length](https://docs.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/json_array_length)

# COMMAND ----------

# MAGIC %sql SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');

# COMMAND ----------

# MAGIC %sql lateral view 
