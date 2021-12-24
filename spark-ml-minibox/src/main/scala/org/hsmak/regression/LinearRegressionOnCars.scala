package org.hsmak.regression

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * ToDo:
  *   - How to perform polynomial regression?
  *   - How to find whether a feature has a quadratic effect?
  *   - how to find correlation between different features, their quadratic or more effect, etc?
  */
object LinearRegressionOnCars extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("LinearRegressionOnCars")
    .getOrCreate()


   /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val carMileageDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/car-mileage.csv")


  println("carMileageDF has " + carMileageDF.count() + " rows")
  carMileageDF.show(5)
  carMileageDF.printSchema()

  // Let us find summary statistics
  carMileageDF.describe("mpg", "hp", "weight", "automatic").show()


   /* ******************************************
    * ############ Linear Regression ###########
    * ******************************************/


   /* ******************************************
    * ########## Feature Transformer ###########
    * ******************************************/

  /**
    * SparkException: Encountered null while assembling a row with handleInvalid = "keep".
    * Consider removing nulls from dataset or using handleInvalid = "keep" or "skip".
    *
    */
  val carsNoNullDF = carMileageDF.na.drop() // Returns a new `DataFrame` that drops rows containing any null or NaN values.

  println(
    s"""Orig = ${carMileageDF.count()}
       |Final = ${carsNoNullDF.count()}
       |Dropped = ${(carMileageDF.count() - carsNoNullDF.count())}""".stripMargin)

  //A feature transformer that merges multiple columns into a vector column.
  //ToDo - This is inefficient in case there are thousands of columns
  val assembler = new VectorAssembler()
    .setInputCols(Array("displacement", "hp", "torque", "CRatio", "RARatio", "CarbBarrells", "NoOfSpeed", "length", "width", "weight", "automatic"))
    .setOutputCol("features") // merges multiple columns into a vector column; i.e. extracting features

  val carsExtractedFeatures = assembler.transform(carsNoNullDF)
  carsExtractedFeatures.show(40)

   /* *************************************************************
    * ############ Data Splitting: Training & Test Sets ###########
    * *************************************************************/


  val train = carsExtractedFeatures.filter(carsNoNullDF("weight") <= 4000)
  val test = carsExtractedFeatures.filter(carsNoNullDF("weight") > 4000)
  test.show()

  println("Train = " + train.count() + " Test = " + test.count())

   /* *****************************************
    * ########## Linear Regression ############
    * *****************************************/

  val lr = new LinearRegression()
    .setMaxIter(100)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setLabelCol("mpg")

   /* ******************************************
    * ############ Training Data ###########
    * ******************************************/

  val lrTrained = lr.fit(train) // Most of the time-consuming work is happening here

  /**
    * Coefficients = Theta values?
    * Intercept = mpg value when others features are zero?
    */
  println(s"Coefficients: ${lrTrained.coefficients} Intercept: ${lrTrained.intercept}")

  val lrTrainedSummary = lrTrained.summary
  println(s"numIterations: ${lrTrainedSummary.totalIterations}")
  println(s"Iteration Summary History: ${lrTrainedSummary.objectiveHistory.toList}")

  lrTrainedSummary.residuals.show()
  println(s"RMSE: ${lrTrainedSummary.rootMeanSquaredError}")
  println(s"r2: ${lrTrainedSummary.r2}")

   /* ************************************************
    * ############ Prediction & Evaluating ###########
    * ************************************************/

  // Now let us use the model to predict our test set
  val predictions = lrTrained.transform(test)
  predictions.show()

  // Calculate RMSE & MSE
  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol("mpg")
  val rmse = evaluator.evaluate(predictions)
  println("Root Mean Squared Error = " + "%6.3f".format(rmse))

  evaluator.setMetricName("mse")
  val mse = evaluator.evaluate(predictions)
  println("Mean Squared Error = " + "%6.3f".format(mse))
}
