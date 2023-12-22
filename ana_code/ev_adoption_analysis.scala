//finding mean value of odometer reading in used cars
//finding percentage of high odometer reading cars
//to see if people make purchase decision for EV based on odometer readings 

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType

def readCSV(path: String): DataFrame = {
  val df = spark.read
    .format("csv")
    .option("header", true) // Set to true if your CSV file has a header
    .option("inferSchema", true) // Set to true to infer the data types of columns
    .load(path)

  df
}

val filePath = "finalproject/Cleaned_Data/part-00000-cc2f2292-25e9-475c-8d2d-9107b992724b-c000.csv"

var EV_data = readCSV(filePath)

def calculateStatistics(inputDF: DataFrame, columnName: String): String = {
  val transformedDF = inputDF
    .withColumn(columnName, col(columnName).cast(DoubleType))
    .na.drop(Seq(columnName))

  val meanValue = transformedDF.select(mean(col(columnName))).as[Double].first()
  val medianValue = transformedDF.stat.approxQuantile(columnName, Array(0.5), 0.01)(0)
  val modeValue = transformedDF.groupBy(col(columnName))
    .agg(count(col(columnName)).alias("count"))
    .sort(desc("count"))
    .limit(1)
    .select(col(columnName))
    .collect()
    .map(_.get(0).toString)
    .mkString(", ")
  val stddevValue = transformedDF.select(stddev(col(columnName))).as[Double].first()

  s"Column: $columnName, Mean: $meanValue, Median: $medianValue, Mode: $modeValue, Standard Deviation: $stddevValue"
}

calculateStatistics(EV_data, "Odometer Reading")

EV_data.count()
//output: Long = 832179

//should not count odometer reading of new cars 
//should not add if odometer reading = 0
val meanValue: Double = EV_data.filter(col("Odometer Reading").isNotNull && col("Odometer Reading") =!= 0 && col("New or Used Vehicle") === "Used").agg(mean(col("Odometer Reading").cast("double")).alias("mean_odometer")).head().getAs[Double](0)

//checking of odometer reading is greater than mean value
var dFwithBinary: DataFrame = EV_data.withColumn("binaryColumn", when(col("Odometer Reading").cast("double") > meanValue, 1).otherwise(0))

dFwithBinary.show()

val countsDF: DataFrame = dFwithBinary.filter(col("New or Used Vehicle") === "Used").groupBy("binaryColumn").count()

countsDF.show()

//finding percentage:
val totalRows = dFwithBinary.filter(col("New or Used Vehicle") === "Used").count()
val percentageDF: DataFrame = countsDF.withColumn("percentage", (col("count") / totalRows * 100).cast("double"))

println(s"The percentage of cars purchased with high odometer range is percentage of binary column 1")
println(s"The percentage of cars purchased with low odometer range is percentage of binary column 0")

percentageDF.show()

//results
//93% of old cars are greater than mean value of odometer reading

//final result is stored in HDFS
dFwithBinary.coalesce(1).write.option("header","true").mode("overwrite").csv("finalproject/odometer_analysis")



