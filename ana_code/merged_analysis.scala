//importing all the neccessary files
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DoubleType

val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("final/merged_data.csv")

//print schema
data.printSchema()

//number of rows and columns
val numRows = data.count()
val numCols = data.columns.length


// Top 5 regions with the highest average demand
val top_demand_regions = data.orderBy(col("avg(Demand (MW)_imputed)").desc).select("Region", "Year", "avg(Demand (MW)_imputed)").limit(10)
println("Top 10 regions with the highest average demand:")
top_demand_regions.show()

// Top 5 regions with the highest average pricing
val top_price_regions = data.orderBy(col("avg(avg_rate)").desc).select("Region","Year","avg(avg_rate)").limit(10)
println("Top 10 regions with the highest average pricing:")
top_price_regions.show()

// Top 5 regions with the highest average EV adoption
val top_ev_regions = data.orderBy(col("EV_adoption_count").desc).select("Region","Year","EV_adoption_count").limit(10)
println("Top 10 regions with the highest average EV adoption:")
top_ev_regions.show()

// Agregatting all the years by region to see the correlation
val analytic_result = data.groupBy("Region").agg(
  avg("avg(Demand (MW)_imputed)").alias("avg_demand"),
  avg("avg(avg_rate)").alias("avg_price"),
  avg("EV_adoption_count").alias("avg_ev_adoption")
).orderBy(col("avg_price").desc)
analytic_result.show()

// correlation matrix for aggregated data over the years
val correlation_matrix = analytic_result.select(
  corr("avg_demand", "avg_price").alias("Corr_demand_price"),
  corr("avg_demand", "avg_ev_adoption").alias("Corr_demand_ev"),
  corr("avg_price", "avg_ev_adoption").alias("Corr_price_ev")
).show()


