//importing all the neccessary files
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DoubleType

//function to clean data
def cleanAndConsolidate(filePath: String): DataFrame = {
  val spark = SparkSession.builder.appName("Cleaning").getOrCreate()

  // Loading and Cleaning the Data
  val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath)

  // Defining the function to clean a string column
  def cleanStringColumn(df: DataFrame, colName: String): DataFrame = {
    df.withColumn(colName, regexp_replace(col(colName), ",", ""))
  }

  // List of columns to clean
  val columnsToClean = List("Balancing Authority", "Data Date", "Hour Number", "Local Time at End of Hour", "Demand Forecast (MW)", "Demand (MW)", "Net Generation (MW)", "Region")

  // Apply the cleanStringColumn function to each specified column
  var cleanedData = columnsToClean.foldLeft(data)((accDF, colName) => cleanStringColumn(accDF, colName))

  //-----------------CODE CLEANING-----------------
  // METHOD1: date formatting: Spliting the "Local Time at End of Hour" field into Date and Time
  cleanedData = cleanedData.withColumn("Local Date", split(col("Local Time at End of Hour"), " ").getItem(0))
  cleanedData = cleanedData.withColumn("Local Time", split(col("Local Time at End of Hour"), " ").getItem(1))
  // METHOD2: Binary Column: Creating a binary column based on Demand Forecast and Demand
  cleanedData = cleanedData.withColumn("Forecast Higher Than Demand", when(col("Demand Forecast (MW)") > col("Demand (MW)"), 1).otherwise(0))
  // METHOD3: Adding a column for the year
  cleanedData = cleanedData.withColumn("Year", year(to_date(col("Data Date"), "MM/dd/yyyy")))
  // METHOD4: Adding a column for the month/year only
  cleanedData = cleanedData.withColumn("MonthYear", date_format(to_date(col("Data Date"), "MM/dd/yyyy"), "MM/yyyy"))
  //-------------------------------------------

  // Selecting Desired Columns excluding the original "Local Time at End of Hour"
  val selectedColumns = cleanedData.select("Balancing Authority", "Data Date", "Year", "MonthYear", "Hour Number", "Local Date", "Local Time", "Demand Forecast (MW)",
    "Demand (MW)", "Net Generation (MW)", "Region", "Forecast Higher Than Demand")
  selectedColumns
}

val data2016a = cleanAndConsolidate("final/raw_data7.csv")
val data2016b = cleanAndConsolidate("final/raw_data8.csv")
val data2017a = cleanAndConsolidate("final/raw_data9.csv")
val data2017b = cleanAndConsolidate("final/raw_data10.csv")
val data2018a = cleanAndConsolidate("final/raw_data11.csv")
val data2018b = cleanAndConsolidate("final/raw_data12.csv")
val data2019a = cleanAndConsolidate("final/raw_data1.csv")
val data2019b = cleanAndConsolidate("final/raw_data2.csv")
val data2020a = cleanAndConsolidate("final/raw_data3.csv")
val data2020b = cleanAndConsolidate("final/raw_data4.csv")
val data2021a = cleanAndConsolidate("final/raw_data5.csv")
val data2021b = cleanAndConsolidate("final/raw_data6.csv")


// Concatenate the DataFrames vertically
val finalResult = data2016a.union(data2016b).union(data2017a).union(data2017b).union(data2018a).union(data2018b).union(data2019a).union(data2019b).union(data2020a).union(data2020b).union(data2021a).union(data2021b)

// Add a header to the final result
val header = Seq("Balancing Authority", "Data Date", "Year", "MonthYear", "Hour Number", "Local Date", "Local Time", "Demand Forecast (MW)",
  "Demand (MW)", "Net Generation (MW)", "Region", "Forecast Higher Than Demand")
val finalResultWithHeader = finalResult.toDF(header: _*)

// Save the final result with a header as a single CSV file
finalResultWithHeader.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/cleanedData")



