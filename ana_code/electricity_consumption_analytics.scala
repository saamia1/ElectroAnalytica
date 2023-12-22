//All the libraries for importing 
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.Window

val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("final/final_data.csv")

def calculateStats(df: DataFrame, colName: String): String = {
  val numericDF = df
    .withColumn(colName, col(colName).cast(DoubleType))
    .na.drop(Seq(colName))  

  val meanValue = numericDF.select(mean(col(colName))).as[Double].first()
  val medianValue = numericDF.stat.approxQuantile(colName, Array(0.5), 0.0001).head
  val modeValue = numericDF.groupBy(col(colName))
    .agg(count(col(colName)).alias("count"))
    .sort(desc("count"))
    .limit(1)
    .select(col(colName))
    .collect()
    .map(_.get(0).toString)
    .mkString(", ") 
  val stddevValue = numericDF.select(stddev(col(colName))).as[Double].first()

  s"Column: $colName, Mean: $meanValue, Median: $medianValue, Mode: $modeValue, Standard Deviation: $stddevValue"
}

//Calculating all the stats for the columns 
val statsDemandForecast = calculateStats(data, "Demand Forecast (MW)_imputed")
val statsDemand = calculateStats(data, "Demand (MW)_imputed")
val statsNetGeneration = calculateStats(data, "Net Generation (MW)_imputed")

println(statsDemandForecast)
println(statsDemand)
println(statsNetGeneration)

//Agg by region and Year/ Month / Hour
val result1 = data.groupBy("Year", "Region").agg(avg("Demand Forecast (MW)_imputed"), avg("Demand (MW)_imputed"), avg("Net Generation (MW)_imputed")).orderBy(col("Region"), col("Year").desc)
val result2 = data.groupBy("MonthYear", "Region").agg(avg("Demand Forecast (MW)_imputed"), avg("Demand (MW)_imputed"), avg("Net Generation (MW)_imputed")).orderBy(col("Region"), col("MonthYear").desc)
val result3 = data.groupBy("Hour Number", "Region").agg(avg("Demand Forecast (MW)_imputed"), avg("Demand (MW)_imputed"), avg("Net Generation (MW)_imputed")).orderBy(col("Region"), col("Hour Number").desc) 

result2.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/avg_month")
result3.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/avg_hour")

result1.show()
result2.show()
result3.show()

//analytics to know the growth rate
val demandWindowSpec = Window.partitionBy("Region").orderBy("Year")
val demandWithGrowthRates = result1.withColumn("prev_avg_demand", lag("avg(Demand (MW)_imputed)", 1).over(demandWindowSpec)).withColumn("demand_growth", ((col("avg(Demand (MW)_imputed)") - col("prev_avg_demand")) / col("prev_avg_demand")) * 100).withColumn("prev_avg_net", lag("avg(Net Generation (MW)_imputed)", 1).over(demandWindowSpec)).withColumn("net_growth", ((col("avg(Net Generation (MW)_imputed)") - col("prev_avg_net")) / col("prev_avg_net")) * 100).na.fill(0).drop("prev_avg_net", "prev_avg_demand")

demandWithGrowthRates.show()
demandWithGrowthRates.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/avg_year")
