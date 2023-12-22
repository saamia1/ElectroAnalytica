// Import necessary Spark SQL classes
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType

// Define the function to read CSV
def readCSV(path: String): DataFrame = {
  // Create a SparkSession and read CSV file into a DataFrame
  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(path)

  // Return the DataFrame
  df
}

// Read the CSV file
val filePath = "hw8/EV_and_RegActivity.csv"
var eV_data = readCSV(filePath)

// Select relevant columns
eV_data = eV_data.select(
  "Model Year", "Make", "Model", "Electric Range", "Odometer Reading",
  "Odometer Code", "New or Used Vehicle", "Transaction Year", "State of Residence"
)

// Drop rows with null values in any column
eV_data = eV_data.na.drop()

// Additional cleaning steps
eV_data = eV_data.withColumn("Odometer Reading", col("Odometer Reading").cast(DoubleType))
eV_data = eV_data.withColumn("Transaction Year", col("Transaction Year").cast(DateType))

// State to region mapping
val stateToRegion = Map(
  "WA" -> "NW", "CA" -> "CAL", "UT" -> "SW", "VA" -> "SE", "OR" -> "NW", "TX" -> "TEX", "MI" -> "MIDW", 
  "TN" -> "TEN", "DE" -> "MIDA", "MN" -> "MIDW", "HI" -> "SW", "ID" -> "NW", "AL" -> "SE", "IN" -> "MIDW", 
  "WY" -> "NW", "GA" -> "SE", "FL" -> "FLA", "KY" -> "SE", "DC" -> "MIDA", "MD" -> "MIDA", "AZ" -> "SW", 
  "NE" -> "MIDW", "LA" -> "SE", "PA" -> "NE", "NC" -> "CAR", "NY" -> "NY", "SD" -> "MIDW", "AK" -> "NW", 
  "NM" -> "SW", "MO" -> "MIDW", "CO" -> "SW", "IL" -> "MIDW", "NV" -> "SW", "SC" -> "CAR", "MS" -> "SE", 
  "MT" -> "NW", "ND" -> "MIDW", "OH" -> "MIDW", "MA" -> "NE", "CT" -> "NE", "NJ" -> "NE", "WI" -> "MIDW", 
  "IA" -> "MIDW", "KS" -> "CENT", "OK" -> "SW", "AR" -> "SE", "NH" -> "NE", "ME" -> "NE"
)

// User-defined function (UDF) for mapping state to region
val mapStateToRegion = udf((state: String) => stateToRegion.getOrElse(state, "Unknown"))

// Add 'Region' column to DataFrame
val cleaned_data = eV_data.withColumn("Region", mapStateToRegion($"State of Residence"))

// Show the cleaned DataFrame
cleaned_data.show()


// For joint analysis 
val filteredDF = cleaned_data.filter(col("Transaction Year").between(2016, 2021))

// Filter out rows with "Unknown" region
val intermediateDF = filteredDF.filter(col("Region") =!= "Unknown")

// Count occurrences of each region for each year
val regionCounts = intermediateDF.groupBy("Region", "Transaction Year").agg(count("Region").alias("EV_adoption_count"),sum(when(col("New or Used Vehicle") === "New", 1).otherwise(0)).alias("new_count"),sum(when(col("New or Used Vehicle") === "Used", 1).otherwise(0)).alias("used_count"))

// Calculate percentages for new and used cars
val resultDF = regionCounts.withColumn("new_percentage", col("new_count") / col("EV_adoption_count") * 100).withColumn("used_percentage", col("used_count") / col("EV_adoption_count") * 100).orderBy("Region", "Transaction Year")

resultDF.show()
 
//later used for individual analysis
cleaned_data.coalesce(1).write.option("header", "true").mode("overwrite").csv("finalproject/Cleaned_Data")

//used for merged analysis
resultDF.coalesce(1).write.option("header", "true").mode("overwrite").csv("finalproject/aggregation16-21")
