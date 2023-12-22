import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder.appName("Electricity Rates Analysis").getOrCreate()
import spark.implicits._

// 1. Read and Prepare Data
val dataPath = "FinalProject/aggregated_electricity_rates_by_region/part-00000-29dfebc4-2a82-467f-b3bc-d0e4be1dc2cd-c000.csv"
val df = spark.read.option("header", "true").csv(dataPath)

// 2. Basic Data Exploration
df.printSchema()
df.show()

// 3. Yearly and Regional Analysis
val yearlyRates = df.groupBy("year").agg(avg("avg(comm_rate)"), avg("avg(ind_rate)"), avg("avg(res_rate)"), avg("avg(avg_rate)"))
yearlyRates.show()
val regionalRates = df.groupBy("region").agg(avg("avg(comm_rate)"), avg("avg(ind_rate)"), avg("avg(res_rate)"), avg("avg(avg_rate)"))
regionalRates.show()
// 4. Statistical Analysis
val stats = df.describe("avg(comm_rate)", "avg(ind_rate)", "avg(res_rate)", "avg(avg_rate)")
stats.show()
// 5. Regions with Most Above-Average Residential Rates
val mostAboveAvgResRates = df.groupBy("region").agg(sum("sum(above_avg_res_rate)").alias("total_above_avg_res_rate")).orderBy(desc("total_above_avg_res_rate"))
mostAboveAvgResRates.show()

// 3. Calculate Growth Rates
val windowSpec = Window.partitionBy("region").orderBy("year")

val dataWithGrowthRates = df.withColumn("prev_comm_rate", lag("avg(comm_rate)", 1).over(windowSpec)).withColumn("prev_ind_rate", lag("avg(ind_rate)", 1).over(windowSpec)).withColumn("prev_res_rate", lag("avg(res_rate)", 1).over(windowSpec)).withColumn("prev_avg_rate", lag("avg(avg_rate)", 1).over(windowSpec)).withColumn("comm_rate_growth", ((col("avg(comm_rate)") - col("prev_comm_rate")) / col("prev_comm_rate")) * 100).withColumn("ind_rate_growth", ((col("avg(ind_rate)") - col("prev_ind_rate")) / col("prev_ind_rate")) * 100).withColumn("res_rate_growth", ((col("avg(res_rate)") - col("prev_res_rate")) / col("prev_res_rate")) * 100).withColumn("avg_rate_growth", ((col("avg(avg_rate)") - col("prev_avg_rate")) / col("prev_avg_rate")) * 100).na.fill(0).drop("prev_comm_rate", "prev_ind_rate", "prev_res_rate", "prev_avg_rate")

dataWithGrowthRates.show()
// 5. Save Analysis Results
val growthRatesOutputPath = "FinalProject/growth_rates_by_region"
dataWithGrowthRates.coalesce(1).write.option("header", "true").mode("overwrite").csv(growthRatesOutputPath)

// 6. Close Spark Session
spark.stop()
