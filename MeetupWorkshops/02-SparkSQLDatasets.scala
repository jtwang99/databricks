// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Spark Datasets with Scala
// MAGIC This notebook demonstrates a number of common Spark Dataset functions using Scala. It also demostrates how structure enables developers to express high-level queries that 
// MAGIC are readable and composable. They look like SQL queries you would express, or domain specific language computation you would perform on your data set.
// MAGIC 
// MAGIC Keep this URL or open in the new tab to consult [Dataset API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

// COMMAND ----------

// MAGIC %md ### Setup: Create Sample Data to demonstrate Datasets.

// COMMAND ----------

// Create the case classes for our domain, in this case a department, employee, and DepartmentWithEmployee.
// Simple example to illustrate how structure makes Spark easy to program.
case class Department(id: String, name: String)
case class Employee(firstName: String, lastName: String, email: String, salary: Int)
case class DepartmentWithEmployees(department: Department, employees: Employee)

// Create the Departments
val department1 = new Department("123456", "Computer Science")
val department2 = new Department("345678", "Mechanical Engineering")
//let's give it the same ID so we can do some joins across common keys
val department3 = new Department("123456", "Theater and Drama")
val department4 = new Department("901234", "Indoor Recreation")

// Create the Employees
val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 200000)
val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)

// Create the DepartmentWithEmployees instances from Departments and Employees
val departmentWithEmployees1 = new DepartmentWithEmployees(department1, employee1)
val departmentWithEmployees2 = new DepartmentWithEmployees(department2, employee3)
val departmentWithEmployees3 = new DepartmentWithEmployees(department3, employee4)
val departmentWithEmployees4 = new DepartmentWithEmployees(department4, employee3)

// COMMAND ----------

// MAGIC %md **Create the first Dataset from a List of the Case Classes.**

// COMMAND ----------

val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
//convert it to a dataset
val ds1 = departmentsWithEmployeesSeq1.toDS()

// COMMAND ----------

display(ds1)

// COMMAND ----------

// MAGIC %md Note that once we have as Dataset, we can access individual data items using Scala object's "." notation. For example, I can print the department or access individual item in the department

// COMMAND ----------

println(ds1.head.department)

// COMMAND ----------

println(ds1.head.department.name)

// COMMAND ----------

// MAGIC %md **Create a 2nd Dataset from a List of Case Classes.**

// COMMAND ----------

val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
val ds2 = departmentsWithEmployeesSeq2.toDS()

// COMMAND ----------

display(ds2)

// COMMAND ----------

println(ds1.head.department)

// COMMAND ----------

println(ds1.head.employees)

// COMMAND ----------

// MAGIC %md Same thing as above, I can access individual items.

// COMMAND ----------

println(ds1.head.employees.firstName, ds1.head.employees.email, ds1.head.employees.salary)

// COMMAND ----------

// MAGIC %md Let's do a common operation performed when you have two datasets related or have a common attribute by joining datasets. In this case,
// MAGIC we can do a join over id with a predicate filter. Nice!!!

// COMMAND ----------

display(ds1)

// COMMAND ----------

display(ds2)

// COMMAND ----------

val dsj3 = ds1.join(ds2, ds1("department.id")=== ds2("department.id"))

// COMMAND ----------

display(dsj3)

// COMMAND ----------

val dsj3 = ds1.join(ds2, ds1("department.id")===ds2("department.id") && (ds2("employees.salary") > ds1("employees.salary")))

// COMMAND ----------

display(dsj3)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Working with Datasets

// COMMAND ----------

// MAGIC %md **Union 2 Datasets.**

// COMMAND ----------

val unionDS = ds1.union(ds2)

// COMMAND ----------

display(unionDS)

// COMMAND ----------

// MAGIC %md **Write the Unioned Dataset to a Parquet file.**

// COMMAND ----------

// Remove the file if it exists
dbutils.fs.rm("/tmp/databricks-ds-example.parquet", true)
unionDS.write.parquet("/tmp/databricks-df-example.parquet")

// COMMAND ----------

// MAGIC %md **Read a Dataset from the Parquet file.**
// MAGIC 
// MAGIC Note: we don't have to specify the schema since parquet stores the schema as part of the file.

// COMMAND ----------

val parquetDS = sqlContext.read.parquet("/tmp/databricks-df-example.parquet").as[DepartmentWithEmployees]

// COMMAND ----------

display(parquetDS)

// COMMAND ----------

// MAGIC %md Let's use a pattern matching case class to only extract employees as convert it into a Dataset

// COMMAND ----------

import spark.implicits._

val explodeDS= parquetDS.map( { 
  case DepartmentWithEmployees(dept: Department, employee: Employee) => {
    val firstName = employee.firstName
    val lastName = employee.lastName
    val email = employee.email
    val salary = employee.salary
    Employee(firstName, lastName, email, salary)
  }
})


// COMMAND ----------

explodeDS.cache()

// COMMAND ----------

display(explodeDS)

// COMMAND ----------

//gives us the type
explodeDS

// COMMAND ----------

// MAGIC %md **Use ``filter()`` to return only the rows that match the given predicate.**

// COMMAND ----------

val filterD1 = explodeDS.filter(e=> e.firstName == "matei" || e.firstName == "michael").sort($"lastName".asc)

// COMMAND ----------

display(filterD1)

// COMMAND ----------

val filterDS2 = explodeDS
  .filter($"firstName" === "matei" || $"firstName" === "michael")
  .sort($"lastName".asc)

// COMMAND ----------

display(filterDS2)

// COMMAND ----------

// MAGIC %md **The ``where()`` clause is equivalent to ``filter()``.**

// COMMAND ----------

val whereDS = explodeDS.where(($"firstName" === "matei") || ($"firstName" === "michael")).sort($"lastName".asc)
display(whereDS)

// COMMAND ----------

// MAGIC %md **Replace ``null`` values with `--` using Dataset Na functions.**

// COMMAND ----------

val naFunctions = explodeDS.na
val nonNullDS = naFunctions.fill("--")
display(nonNullDS)

// COMMAND ----------

// MAGIC %md **Retrieve only rows with missing firstName or lastName.**

// COMMAND ----------

val filterNonNullDF = nonNullDF.filter($"firstName" === "--" || $"lastName" === "--").sort($"email".asc)
display(filterNonNullDF)

// COMMAND ----------

// MAGIC %md **Example aggregations using ``agg()`` and ``countDistinct()``.**

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// Find the distinct (firstName, lastName) combinations
val countDistinctDF = nonNullDS.select($"firstName", $"lastName")
  .groupBy($"firstName", $"lastName")
  .agg(countDistinct($"firstName") as "distinct_first_names") 
display(countDistinctDF)

// COMMAND ----------

// MAGIC %md **Compare the DataFrame and SQL Query Physical Plans**
// MAGIC (Hint:  They should be the same.)

// COMMAND ----------

countDistinctDF.explain(true)

// COMMAND ----------

// register the DataFrame as a temp table so that we can query it using SQL
nonNullDF.createOrReplaceTempView("databricks_df_example")
display(spark.catalog.listTables)

// COMMAND ----------

// Perform the same query as the DataFrame above and return ``explain``
val countDistinctSQLDF = spark.sql("""
SELECT firstName, lastName, count(distinct firstName) as distinct_first_names
FROM databricks_df_example
GROUP BY firstName, lastName
""")
display(countDistinctSQLDF)

// COMMAND ----------

countDistinctSQLDF.explain(true)

// COMMAND ----------

// Sum up all the salaries
val salarySumDF = nonNullDF.agg("salary" -> "sum") 
display(salarySumDF)

// COMMAND ----------

// MAGIC %md **Print the summary statistics for the salaries.**

// COMMAND ----------

nonNullDF.describe("salary").show()

// COMMAND ----------

val veryNestedDF = Seq(("1", (2, (3, 4)))).toDF()
veryNestedDF.printSchema
val sch = veryNestedDF.schema

// COMMAND ----------

// MAGIC %md **Cleanup: Remove the parquet file.**

// COMMAND ----------

dbutils.fs.rm("/tmp/databricks-df-example.parquet", true)

// COMMAND ----------

implicit class StringImprovements(val s: String) {
    def hideAll = s.replaceAll(".", "*")
}

// COMMAND ----------

"Jules S. Damji".hideAll
