// Databricks notebook source
val blobAccessKey = dbutils.secrets.get(scope = "myblob", key = "accesskey")
val storageAccountName = "covidblob1020"
val containerName = "covidproject"

spark.conf.set(
  "fs.azure.account.key.covidblob1020.blob.core.windows.net",
  blobAccessKey)


// COMMAND ----------

import org.apache.spark.sql.types._

val customSchema = StructType(Array(
  StructField("year", IntegerType, true), 
  StructField("month", IntegerType, true), 
  StructField("day", IntegerType, true),
  StructField("statefips", IntegerType, true), 
  StructField("emp_combined", FloatType, true), 
  StructField("emp_combined_inclow", FloatType, true), 
  StructField("emp_combined_incmiddle", FloatType, true),
  StructField("emp_combined_inchigh", FloatType, true), 
  StructField("emp_combined_ss40", FloatType, true), 
  StructField("emp_combined_ss60", FloatType, true), 
  StructField("emp_combined_ss65", FloatType, true), 
  StructField("emp_combined_ss70", FloatType, true))
)

// COMMAND ----------

val df = spark.read
  .option("sep", ",")
  .option("header", "true")
  .schema(customSchema)
  .csv("wasbs://covidproject@covidblob1020.blob.core.windows.net/employmentcombined")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

val df2 = df.withColumn("newdate", (concat_ws("-", col("year"), col("month"), col("day"))))
.drop("year").drop("month").drop("day").drop("emp_combined_ss40").drop("emp_combined_ss60").drop("emp_combined_ss65").drop("emp_combined_ss70")

// COMMAND ----------

df2.show(5)

// COMMAND ----------

//Convert to datetype
val timeDF = df2.select(col("newdate"), col("statefips"), col("emp_combined"), col("emp_combined_inclow"), col("emp_combined_incmiddle"), col("emp_combined_inchigh"), to_timestamp(col("newdate"), "yyyy-M-dd").cast(DateType).as("date")).drop("newdate")

// COMMAND ----------

timeDF.printSchema()

// COMMAND ----------

timeDF.show(5)

// COMMAND ----------

//Convert to percentages
val convertDF = timeDF.withColumn("combinedEmployment", (col("emp_combined")) * 100)
  .withColumn("lowIncome", (col("emp_combined_inclow")) * 100)
  .withColumn("middleIncome", (col("emp_combined_incmiddle")) * 100)
  .withColumn("highIncome", (col("emp_combined_inchigh")) * 100)
  .drop("emp_combined")
  .drop("emp_combined_inclow")
  .drop("emp_combined_incmiddle")
  .drop("emp_combined_inchigh")
  .withColumnRenamed("statefips", "stateFips")

// COMMAND ----------

convertDF.show(5)

// COMMAND ----------

val state = List(("Alabama", "AL", 1),
  ("Alaska", "AK", 2),
  ("Arizona", "AZ", 4),
  ("Arkansas", "AR", 5),
  ("California", "CA", 6),
  ("Colorado", "CO", 8),
  ("Connecticut", "CT", 9),
  ("Delaware", "DE", 10),
  ("Florida", "FL", 12),
  ("Georgia", "GA",	13),
  ("Hawaii", "HI", 15),
  ("Idaho",	"ID", 16),
  ("Illinois", "IL", 17),
  ("Indiana", "IN", 18),
  ("Iowa", "IA", 19),
  ("Kansas", "KS", 20),
  ("Kentucky", "KY", 21),
  ("Louisiana",	"LA", 22),
  ("Maine", "ME", 23),
  ("Maryland", "MD", 24),
  ("Massachusetts", "MA", 25),
  ("Michigan", "MI", 26),
  ("Minnesota", "MN", 27),
  ("Mississippi", "MS", 28),
  ("Missouri", "MO", 29),
  ("Montana",	"MT", 30),
  ("Nebraska", "NE", 31),
  ("Nevada", "NV", 32),
  ("New Hampshire", "NH", 33),
  ("New Jersey", "NJ", 34),
  ("New Mexico", "NM", 35),
  ("New York", "NY", 36),
  ("North Carolina", "NC", 37),
  ("North Dakota", "ND", 38),
  ("Ohio", "OH", 39),
  ("Oklahoma", "OK", 40),
  ("Oregon", "OR", 41),
  ("Pennsylvania", "PA", 42),
  ("Rhode Island", "RI", 44),
  ("South Carolina", "SC", 45),
  ("South Dakota", "SD", 46),
  ("Tennessee", "TN", 47),
  ("Texas", "TX", 48),
  ("Utah", "UT", 49),
  ("Vermont", "VT", 50),
  ("Virginia", "VA", 51),
  ("Washington", "WA", 53),
  ("West Virginia",	"WV", 54),
  ("Wisconsin",	"WI", 55),
  ("Wyoming", "WY", 56),
  ("American Samoa", "AS", 60),
  ("Guam", "GU", 66),
  ("Northern Mariana Islands", "MP", 69),
  ("Puerto Rico", "PR", 72),
  ("Virgin Islands", "VI", 78)
  )

// COMMAND ----------

val schema = StructType(Array(StructField("state", StringType, false),
  StructField("stateCode", StringType, false),
  StructField("stateFips", IntegerType, false)
))

// COMMAND ----------

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val rdd = spark.sparkContext.parallelize(state)
val stateDF = rdd.toDF("state", "stateCode", "stateFips")

// COMMAND ----------

stateDF.show(5)

// COMMAND ----------

val VirginiaDF = convertDF.where(col("stateFips") === 51).groupBy("date").avg("combinedEmployment", "lowIncome", "middleIncome", "highIncome") 

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

VirginiaDF.show(5)

// COMMAND ----------

display(VirginiaDF)

// COMMAND ----------

//windowing function for quarter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


val dateDF = convertDF.select("date")
  .withColumn("quarter", quarter(col("date")))

// COMMAND ----------

val dimdateDF = dateDF.dropDuplicates()

// COMMAND ----------

dimdateDF.show(5)

// COMMAND ----------

//write stateDF to SQL DB
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

val url = "jdbc:sqlserver://afssqldb.database.windows.net:1433;database=CovidProject"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", "afssqldb")
connectionProperties.put("password", dbutils.secrets.get(scope = "sqldb", key = "password"))

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)


stateDF.write
  .format("jdbc")
  .jdbc(url, "state", connectionProperties)

// COMMAND ----------

//write dimdateDF to SQL DB
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

val url = "jdbc:sqlserver://afssqldb.database.windows.net:1433;database=CovidProject"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", "afssqldb")
connectionProperties.put("password", dbutils.secrets.get(scope = "sqldb", key = "password"))

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

dimdateDF.write
  .format("jdbc")
  .jdbc(url, "date", connectionProperties)

// COMMAND ----------

//write convertDF to SQL DB
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

val url = "jdbc:sqlserver://afssqldb.database.windows.net:1433;database=CovidProject"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", "afssqldb")
connectionProperties.put("password", dbutils.secrets.get(scope = "sqldb", key = "password"))

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

convertDF.write
  .format("jdbc")
  .jdbc(url, "combinedEmployment", connectionProperties)

// COMMAND ----------

//write VirginiaDF to SQL DB
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

val url = "jdbc:sqlserver://afssqldb.database.windows.net:1433;database=CovidProject"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", "afssqldb")
connectionProperties.put("password", dbutils.secrets.get(scope = "sqldb", key = "password"))

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

VirginiaDF.write
  .format("jdbc")
  .jdbc(url, "Virginia", connectionProperties)
