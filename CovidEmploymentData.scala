// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

//Define schema
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

//Reading data from Azure Data Lake Storage using credential passthrough
val df = spark
  .read
  .option("sep", ",")
  .option("header", "true")
  .schema(customSchema)
  .csv("adl://mydlsgen1.azuredatalakestore.net/CovidER/EmploymentCombinedStateDaily.csv")

// COMMAND ----------

//Clean up columns
val df2 = df.withColumn("newdate", (concat_ws("-", col("year"), col("month"), col("day"))))
.drop("year").drop("month").drop("day").drop("emp_combined_ss40").drop("emp_combined_ss60").drop("emp_combined_ss65").drop("emp_combined_ss70")

// COMMAND ----------

//Create dataframe of sector employment data
val sDF = df.select("year", "month", "day", "statefips", "emp_combined_ss40", "emp_combined_ss60", "emp_combined_ss65", "emp_combined_ss70")
  .withColumn("newdate", (concat_ws("-", col("year"), col("month"), col("day"))))
  .drop("year", "month", "day")

// COMMAND ----------

//Fix data types
val sectorDF = sDF.select(col("newdate"), col("statefips"), col("emp_combined_ss40"), col("emp_combined_ss60"), col("emp_combined_ss65"), col("emp_combined_ss70"), to_timestamp(col("newdate"), "yyyy-M-dd").cast(DateType).as("date"))
  .withColumn("TradeTransportationUtilities", (col("emp_combined_ss40")) * 100)
  .withColumn("ProfessionalandBusinessServices", (col("emp_combined_ss60")) * 100)
  .withColumn("EducationandHealthServices", (col("emp_combined_ss65")) * 100)
  .withColumn("LeisureandHospitality", (col("emp_combined_ss70")) * 100)
  .drop("emp_combined_ss40")
  .drop("emp_combined_ss60")
  .drop("emp_combined_ss65")
  .drop("emp_combined_ss70")
  .drop("newdate")

// COMMAND ----------

//Fix data types
val timeDF = df2.select(col("newdate"), col("statefips"), col("emp_combined"), col("emp_combined_inclow"), col("emp_combined_incmiddle"), col("emp_combined_inchigh"), to_timestamp(col("newdate"), "yyyy-M-dd").cast(DateType).as("date")).drop("newdate")

// COMMAND ----------

//Create percentages
val nationalDF = timeDF.withColumn("combinedEmployment", (col("emp_combined")) * 100)
  .withColumn("lowIncome", (col("emp_combined_inclow")) * 100)
  .withColumn("middleIncome", (col("emp_combined_incmiddle")) * 100)
  .withColumn("highIncome", (col("emp_combined_inchigh")) * 100)
  .drop("emp_combined")
  .drop("emp_combined_inclow")
  .drop("emp_combined_incmiddle")
  .drop("emp_combined_inchigh")
  .withColumnRenamed("statefips", "stateFips")

// COMMAND ----------

//Windowing function for quarter
val dateDF = nationalDF.select("date")
  .withColumn("quarter", quarter(col("date")))

// COMMAND ----------

val dimdateDF = dateDF.dropDuplicates()

// COMMAND ----------

//List of State and state fips reference
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

//Create schema
val schema = StructType(Array(StructField("state", StringType, false),
  StructField("stateCode", StringType, false),
  StructField("stateFips", IntegerType, false)
))

// COMMAND ----------

//Create dataframe from list
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val rdd = spark.sparkContext.parallelize(state)
val dimstateDF = rdd.toDF("state", "stateCode", "stateFips")

// COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.conf.set("fs.azure.account.key.dwpool.blob.core.windows.net", "/VHS4BskHOFMEsaNqkvxpcrojhkEMK5tv2xLaoeBiP9b6TjhtfyGoL1CfQl59Q6u2aL9Gpps5HYEfECKcIS2Yg==")

// COMMAND ----------

//Write dataframes to SQL pool
nationalDF.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://afssqldw.database.windows.net:1433;database=afssqldw2;user=afsmithcodes@afssqldw;password=LOVE4life$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "National")
  .option("tempDir", "wasbs://coviderdw@dwpool.blob.core.windows.net/tempDirs")
  .save()

// COMMAND ----------

sectorDF.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://afssqldw.database.windows.net:1433;database=afssqldw2;user=afsmithcodes@afssqldw;password=LOVE4life$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "Sector")
  .option("tempDir", "wasbs://coviderdw@dwpool.blob.core.windows.net/tempDirs")
  .save()

// COMMAND ----------

dimdateDF.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://afssqldw.database.windows.net:1433;database=afssqldw2;user=afsmithcodes@afssqldw;password=LOVE4life$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "Date")
  .option("tempDir", "wasbs://coviderdw@dwpool.blob.core.windows.net/tempDirs")
  .save()

// COMMAND ----------

dimstateDF.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://afssqldw.database.windows.net:1433;database=afssqldw2;user=afsmithcodes@afssqldw;password=LOVE4life$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "State")
  .option("tempDir", "wasbs://coviderdw@dwpool.blob.core.windows.net/tempDirs")
  .save()
