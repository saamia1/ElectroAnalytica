//importing all the neccessary files
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.expressions.Window
val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("final/final_data.csv")

//saving all the values as type double 
var cleandf = data.withColumn("Demand Forecast (MW)", col("Demand Forecast (MW)").cast(DoubleType)).withColumn("Demand (MW)", col("Demand (MW)").cast(DoubleType)).withColumn("Net Generation (MW)", col("Net Generation (MW)").cast(DoubleType))

// Show the schema of the DataFrame
cleandf.printSchema()

// Show the number of rows and columns in the DataFrame
val numRows = cleandf.count()
val numCols = cleandf.columns.length

println(s"Number of rows: $numRows")
println(s"Number of columns: $numCols")

//checking the amount of null values in the respective columns
val emptyValuesCount = cleandf
  .select(
    count(when(col("Demand Forecast (MW)").isNull || isnan(col("Demand Forecast (MW)")), true)).alias("Empty_Demand_Forecast"),
    count(when(col("Demand (MW)").isNull || isnan(col("Demand (MW)")), true)).alias("Empty_Demand"),
    count(when(col("Net Generation (MW)").isNull || isnan(col("Net Generation (MW)")), true)).alias("Empty_Net_Generation")
  )
  .show()

//looking at the correlation between net generation and demand/demand forecast
val correlationMatrix = cleandf
  .select(
    corr("Demand Forecast (MW)", "Net Generation (MW)").alias("Corr_Demand_Forecast_Net_Generation"),
    corr("Demand (MW)", "Net Generation (MW)").alias("Corr_Demand_Net_Generation")
  )
  .show()

//--------------------TRYING TO IMPUTE DATA 1------------------------ 

//METHOD 1: Trying to build an linear regression model to impute the values
//choosing net generation as the feature and demand as the lable 
//since the number of null values of net gen is very minimal, we drop that for analysis
val completeData = cleandf.na.drop("all", Seq("Net Generation (MW)")) 

//train test split for linear regression model 
val trainingData = completeData.filter(col("Demand (MW)").isNotNull)
val testData = completeData.filter(col("Demand (MW)").isNull)

//building the model and setting the feature and label for the model
val assembler = new VectorAssembler().setInputCols(Array("Net Generation (MW)")).setOutputCol("features") //creating this as the model only takes in vectors
val model = new LinearRegression().setFeaturesCol("features").setLabelCol("Demand (MW)").fit(assembler.transform(trainingData))

// Evaluate the model on the training set itself for accuracy 
val predictions = model.transform(assembler.transform(trainingData))

// Evaluate the model on the training set for accuracy
val evaluator = new RegressionEvaluator().setLabelCol("Demand (MW)").setPredictionCol("prediction").setMetricName("r2") 
val r2 = evaluator.evaluate(predictions)
println(s"Goodness of fit on training data: $rmse")
//As the correlation actual showed to us that the correlation is very low so the accuracy of the model is also low
//would have to look into more sofisticated models so we just imputed the values by the mean

//--------------------TRYING TO IMPUTE DATA 2------------------------ 

//METHOD 2: Mean value imputation
val columnsToImpute = Array("Demand Forecast (MW)", "Demand (MW)", "Net Generation (MW)")
val imputer = new Imputer().setInputCols(columnsToImpute).setOutputCols(columnsToImpute.map(col => s"${col}_imputed")).setStrategy("mean")
val imputedData = imputer.fit(cleandf).transform(cleandf)
imputedData.show()

imputedData.coalesce(1).write.option("header", "true").mode("overwrite").csv("final/imputedClean")

