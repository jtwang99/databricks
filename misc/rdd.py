# Databricks notebook source
# Bazic map example in python
x = sc.parallelize(["spark rdd example", "sample example"], 2)
 
# map operation will return Array of Arrays in following case (check the result)
y = x.map(lambda x: x.split(' '))
print(y.collect())
# [['spark', 'rdd', 'example'], ['sample', 'example']]
 
# flatMap operation will return Array of words in following case (check the result)
y = x.flatMap(lambda x: x.split(' '))
print(y.collect())
# ['spark', 'rdd', 'example', 'sample', 'example']

# COMMAND ----------

import pyspark
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages),df.properties).toDF('name', 'knownLanguages', 'properties')
df.printSchema()
df2.printSchema()
df2.show()


# COMMAND ----------

display(df)

# COMMAND ----------

df3 = df2.select(df2.name, df2.knownLanguages, explode(df2.properties))
df3.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC import org.apache.spark.SparkContext
# MAGIC import org.apache.spark.SparkConf
# MAGIC import org.apache.spark.sql.SQLContext
# MAGIC import org.apache.spark.sql.types.{StructType,ArrayType}
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.Column
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.functions.explode_outer
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC object FlattenJson extends App {
# MAGIC   val conf = new SparkConf().setMaster("local[*]").setAppName("JSON Flattener")
# MAGIC   val sc = new SparkContext(conf)
# MAGIC   
# MAGIC   val sqlContext = new SQLContext(sc)
# MAGIC   import sqlContext.implicits._
# MAGIC   
# MAGIC   val inputJson = """|{
# MAGIC                      | "name":"John",
# MAGIC                      | "age":30,
# MAGIC                      | "bike":{
# MAGIC                      |	"name":"Bajaj", "models":["Dominor", "Pulsar"]
# MAGIC                      |	},
# MAGIC                      | "cars": [
# MAGIC                      |   { "name":"Ford", "models":[ "Fiesta", "Focus", "Mustang" ] },
# MAGIC                      |   { "name":"BMW", "models":[ "320", "X3", "X5" ] },
# MAGIC                      |   { "name":"Fiat", "models":[ "500", "Panda" ] }
# MAGIC                      | ]
# MAGIC                      |}""".stripMargin('|')
# MAGIC   println(inputJson)
# MAGIC   
# MAGIC   //creating rdd for the json
# MAGIC   val jsonRDD = sc.parallelize(inputJson::Nil)
# MAGIC   //creating DF for the json
# MAGIC   val jsonDF = sqlContext.read.json(jsonRDD)
# MAGIC  
# MAGIC   //Schema of the JSON DataFrame before Flattening
# MAGIC   jsonDF.schema
# MAGIC   
# MAGIC   //Output DataFrame Before Flattening
# MAGIC   jsonDF.show(false)
# MAGIC   
# MAGIC   //Function for exploding Array and StructType column
# MAGIC   
# MAGIC   def flattenDataframe(df: DataFrame): DataFrame = {
# MAGIC 
# MAGIC     val fields = df.schema.fields
# MAGIC     val fieldNames = fields.map(x => x.name)
# MAGIC     val length = fields.length
# MAGIC     
# MAGIC     for(i <- 0 to fields.length-1){
# MAGIC       val field = fields(i)
# MAGIC       val fieldtype = field.dataType
# MAGIC       val fieldName = field.name
# MAGIC       fieldtype match {
# MAGIC         case arrayType: ArrayType =>
# MAGIC           val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
# MAGIC           val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
# MAGIC          // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
# MAGIC           val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
# MAGIC           return flattenDataframe(explodedDf)
# MAGIC         case structType: StructType =>
# MAGIC           val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
# MAGIC           val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
# MAGIC           val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
# MAGIC          val explodedf = df.select(renamedcols:_*)
# MAGIC           return flattenDataframe(explodedf)
# MAGIC         case _ =>
# MAGIC       }
# MAGIC     }
# MAGIC     df
# MAGIC   }
# MAGIC   
# MAGIC   val flattendedJSON = flattenDataframe(jsonDF)
# MAGIC   //schema of the JSON after Flattening
# MAGIC   flattendedJSON.schema
# MAGIC   
# MAGIC   //Output DataFrame After Flattening
# MAGIC   flattendedJSON.show(false)
# MAGIC   
# MAGIC }

# COMMAND ----------

flattenDataframe(df)

# COMMAND ----------


