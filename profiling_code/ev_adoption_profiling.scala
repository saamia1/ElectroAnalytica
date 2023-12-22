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

val filePath = "finalproject/Cleaned_Data/part-00000-6de20ac7-e57c-426f-b9e3-e44fc1e7f387-c000.csv"

var profiled_data = readCSV(filePath)

// Display basic information about the DataFrame
println("DataFrame Schema:")
profiled_data.printSchema()

println("\nNumber of Rows and Columns in the DataFrame:")
println(s"Rows: ${profiled_data.count()}, Columns: ${profiled_data.columns.length}")

// Count of null values in each column
println("\nCount of null values in each column:")
profiled_data.select(profiled_data.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show()

// Analyze how many 'Odometer Reading' values are equal to 0
val zeroOdometerCount = profiled_data.filter(col("Odometer Reading") === 0).count()
println(s"\nNumber of rows with 'Odometer Reading' equal to 0: $zeroOdometerCount")

// Calculate total 'Odometer Reading' count
val totalOdometerCount = profiled_data.filter(col("Odometer Reading").isNotNull).count()
println(s"\nTotal 'Odometer Reading' count: $totalOdometerCount")

// Calculate percentage of 'Odometer Reading' values equal to 0
val percentageZeroOdometer = (zeroOdometerCount.toDouble / totalOdometerCount) * 100
println(f"\nPercentage of 'Odometer Reading' equal to 0: $percentageZeroOdometer%.2f%%")

// Show the DataFrame
profiled_data.show()


