import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.Imputer


val spark = SparkSession.builder.appName("Electricity Rates Profiling").getOrCreate()

// Function to read CSV and add year column
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

// Data Profiling
// Show the schema of the DataFrame
dataDF.printSchema()

// Show the number of rows in the DataFrame
val numRows = dataDF.count()
println(s"Number of rows: $numRows")

// Check for null values in each column
// Check for null values in each column
dataDF.columns.foreach { colName =>
  val nullCount = dataDF.select(count(when(col(colName).isNull || isnan(col(colName)), true)).alias(s"Nulls_in_$colName"))
  nullCount.show()
}

// Instantiate the Imputer transformer to fill in missing values with the mean
val columnsToImpute = Array("comm_rate", "ind_rate", "res_rate")
val imputer = new Imputer()
  .setInputCols(columnsToImpute)
  .setOutputCols(columnsToImpute.map(c => s"${c}_imputed"))
  .setStrategy("mean") // Use the mean value of the column for imputation

// Apply the Imputer transformer to the DataFrame with null values
val imputedData = imputer.fit(dataDF).transform(dataDF)

// Show the result with imputed values
imputedData.show()

imputedData.coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv("/user/ar7165_nyu_edu/FinalProject/imputed_data")

// Stop the Spark session
spark.stop()
