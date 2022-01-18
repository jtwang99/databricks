# Databricks notebook source
# import findspark
# findspark.init()
# findspark.find()
# import pyspark
# findspark.find()
from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType, BooleanType, DoubleType
from pyspark.sql import Row


#UDF to call Rest API
def restApiCall(action, url):
  response = None
  try:
    if action == "get":
      response = requests.get(url)
    else:
      print("Not a get request")
  except Exception as e:
    return e

  if response != None and response.status_code == 200:
    return response.json()

  return None


schema1=StructType([
    StructField("dataset",StructType([
        StructField("id",LongType()),
        StructField("dataset_code",StringType()),
        StructField("database_code",StringType()),
        StructField("name",StringType()),
        StructField("description",StringType()),
        StructField("refreshed_at",StringType()),
        StructField("newest_available_date",StringType()),
        StructField("oldest_available_date",StringType()),
        StructField("column_names",ArrayType(StringType())),
        StructField("frequency",StringType()),
        StructField("type",StringType()),
        StructField("premium",BooleanType()),
        StructField("limit",StringType()),
        StructField("transform",StringType()),
        StructField("column_index",StringType()),
        StructField("start_date",StringType()),
        StructField("end_date",StringType()),
        StructField("data", ArrayType(
            StructType([StructField('Dater',StringType(),True),
                        StructField('Open',DoubleType(),True),
                        StructField('High',DoubleType(),True),
                        StructField('Low',DoubleType(),True),
                        StructField('Close',DoubleType(),True),
                        StructField('Volume',DoubleType(),True),
                        StructField('Ex-dividend',DoubleType(),True),
                        StructField('SplitRatio',DoubleType(),True),
                        StructField('AdjOpen',DoubleType(),True),
                        StructField('AdjHigh',DoubleType(),True),
                        StructField('AdjLow',DoubleType(),True),
                        StructField('AdjClose',DoubleType(),True),
                        StructField('AdjVolume',DoubleType(),True)
                       ])
        )),
        StructField("collapse",StringType()),
        StructField("order",StringType()),
        StructField("database_id",LongType())
    ]))
])


udf_restApiCall = udf(restApiCall, schema1)

# COMMAND ----------

url_list = ["https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.json?start_date=1997-10-01&end_date=1997-10-05&order=asc","https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.json?start_date=1997-11-01&end_date=1997-11-05&order=asc"]
urls = []
for url in url_list:
    urls.append(Row("get",url))
    
print(urls)    

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
sch_url=StructType([StructField('action',StringType()),StructField('url',StringType())])
api_df = spark.createDataFrame(urls,sch_url).repartition(3).withColumn('pid',spark_partition_id())
api_df.show()




# COMMAND ----------

api_df_data = api_df.withColumn("data_col",udf_restApiCall(col('action'),col('url')))
api_explode = api_df_data.select('pid',explode('data_col.dataset.data').alias('exploded'))
api_explode.show(40)

# COMMAND ----------

api_formatted = api_explode.select('exploded.*')
api_formatted.printSchema()
api_formatted.show()

# COMMAND ----------

display(api_explode)

# COMMAND ----------


