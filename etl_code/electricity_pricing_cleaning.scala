import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.rdd.RDD

// Initialize Spark Session
val spark = SparkSession.builder.appName("Data Cleaning for Electricity Rates").getOrCreate()
import spark.implicits._

// Define the state-to-region mapping
val stateToRegion = Map(
  "WA" -> "NW", "CA" -> "CAL", "UT" -> "SW", "VA" -> "SE", "OR" -> "NW", "TX" -> "TEX", "MI" -> "MIDW", 
  "TN" -> "TEN", "DE" -> "MIDA", "MN" -> "MIDW", "HI" -> "SW", "ID" -> "NW", "AL" -> "SE", "IN" -> "MIDW", 
  "WY" -> "NW", "GA" -> "SE", "FL" -> "FLA", "KY" -> "SE", "DC" -> "MIDA", "MD" -> "MIDA", "AZ" -> "SW", 
  "NE" -> "MIDW", "LA" -> "SE", "PA" -> "NE", "NC" -> "CAR", "NY" -> "NY", "SD" -> "MIDW", "AK" -> "NW", 
  "NM" -> "SW", "MO" -> "MIDW", "CO" -> "SW", "IL" -> "MIDW", "NV" -> "SW", "SC" -> "CAR", "MS" -> "SE", 
  "MT" -> "NW", "ND" -> "MIDW", "OH" -> "MIDW", "MA" -> "NE", "CT" -> "NE", "NJ" -> "NE", "WI" -> "MIDW", 
  "IA" -> "MIDW", "KS" -> "CENT", "OK" -> "SW", "AR" -> "SE", "NH" -> "NE", "ME" -> "NE"
)

// Function to map state to region
val mapStateToRegion = udf((state: String) => stateToRegion.getOrElse(state, null))

// Function to read CSV, add year, and optionally skip the first row
def readCsvWithYearAndSkipFirstRow(path: String, year: Int, skipFirstRow: Boolean): RDD[String] = {
  val rdd = spark.sparkContext.textFile(path).mapPartitionsWithIndex { (idx, iter) =>
    if (idx == 0 && skipFirstRow) iter.drop(1) else iter
  }
  rdd.map(row => s"$row,$year")
}

// Define paths to your CSV files
val basePath = "/user/ar7165_nyu_edu/HW8"
val path2016 = s"$basePath/electricity_rates_2016.csv"
val path2017 = s"$basePath/electricity_rates_2017.csv"
val path2018 = s"$basePath/electricity_rates_2018.csv"
val path2019 = s"$basePath/electricity_rates_2019.csv"
val path2020 = s"$basePath/electricity_rates_2020.csv"
val path2021 = s"$basePath/electricity_rates_2021.csv"

// Read CSV files and add the year column
val rdd2016 = readCsvWithYearAndSkipFirstRow(path2016, 2016, skipFirstRow = false)
val rdd2017 = readCsvWithYearAndSkipFirstRow(path2017, 2017, skipFirstRow = true)
val rdd2018 = readCsvWithYearAndSkipFirstRow(path2018, 2018, skipFirstRow = true)
val rdd2019 = readCsvWithYearAndSkipFirstRow(path2019, 2019, skipFirstRow = true)
val rdd2020 = readCsvWithYearAndSkipFirstRow(path2020, 2020, skipFirstRow = true)
val rdd2021 = readCsvWithYearAndSkipFirstRow(path2021, 2021, skipFirstRow = true)

// Merge RDDs
val mergedRDD = rdd2016.union(rdd2017).union(rdd2018).union(rdd2019).union(rdd2020).union(rdd2021)

// Convert RDD to DataFrame and split columns
val mergedDF = mergedRDD.toDF("value")
val splitCols = split($"value", ",")

val dataDF = mergedDF.select(
  splitCols.getItem(0).as("zip"),
  splitCols.getItem(1).as("eiaid"),
  splitCols.getItem(2).as("utility_name"),
  splitCols.getItem(3).as("state"),
  splitCols.getItem(6).cast("double").as("comm_rate"),
  splitCols.getItem(7).cast("double").as("ind_rate"),
  splitCols.getItem(8).cast("double").as("res_rate"),
  splitCols.getItem(9).as("year")
)

// Cleaning Data: Dropping Null Values
val cleanedDF = dataDF.na.drop()

// Calculate average rate and identify above average residential rates
val nationalAvgResRate = 0.12
val dataWithAvgRate = cleanedDF.withColumn(
  "avg_rate",
  (col("comm_rate") + col("ind_rate") + col("res_rate")) / 3
).withColumn(
  "above_avg_res_rate",
  when($"res_rate" > nationalAvgResRate, 1).otherwise(0)
)

// Add the 'region' column to the DataFrame
val dataWithRegion = dataWithAvgRate.withColumn("region", mapStateToRegion(col("state"))).filter($"region".isNotNull && length(trim($"region")) > 0)

val aggregatedData = dataWithRegion.groupBy("region","year").agg(
  avg("comm_rate"),
  avg("ind_rate"),
  avg("res_rate"),
  avg("avg_rate"),
  sum("above_avg_res_rate")
)

val aggregatedOutputPath = s"/user/ar7165_nyu_edu/FinalProject/aggregated_electricity_rates_by_region"
aggregatedData
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(aggregatedOutputPath)
