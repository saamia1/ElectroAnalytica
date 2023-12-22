import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// Define the paths to the CSV files
val consumptionDataPath = "/user/svn9705_nyu_edu/final/consumption_agg_data.csv"
val pricingDataPath = "/user/ar7165_nyu_edu/FinalProject/pricing_agg_data.csv"
val vehiclelDataPath = "/user/ss14758_nyu_edu/finalproject/vehicle_agg_data.csv"

val consumptionDF = spark.read.option("header", "true").option("inferSchema", "true").csv(consumptionDataPath)
val pricingDF = spark.read.option("header", "true").option("inferSchema", "true").csv(pricingDataPath)
val vehicleDF = spark.read.option("header", "true").option("inferSchema", "true").csv(vehiclelDataPath)

// Merge the datasets on 'region' and 'year' columns
val mergedDF = consumptionDF.join(pricingDF,
  consumptionDF("Region") === pricingDF("region") &&
  consumptionDF("Year") === pricingDF("year"),
  "inner"
).join(vehicleDF,
  consumptionDF("Region") === vehicleDF("Region") &&
  consumptionDF("Year") === vehicleDF("Transaction Year"),
  "inner"
)

// Select the desired columns from both DataFrames
// You might need to adjust this list to include the columns you need
val finalDF = mergedDF.select(
  consumptionDF("Region"),
  consumptionDF("Year"),
  consumptionDF("avg(Demand (MW)_imputed)"),
  consumptionDF("avg(Net Generation (MW)_imputed)"),
  pricingDF("avg(avg_rate)"),
  vehicleDF("EV_adoption_count")
)
finalDF.show()

finalDF.write.option("header", "true").mode("overwrite").csv("/user/ar7165_nyu_edu/FinalProject/mergedData")
finalDF.write.option("header", "true").mode("overwrite").csv("/user/ss14758_nyu_edu/final_mergedData")
finalDF.write.option("header", "true").mode("overwrite").csv("/user/svn9705_nyu_edu/final/mergedData")

