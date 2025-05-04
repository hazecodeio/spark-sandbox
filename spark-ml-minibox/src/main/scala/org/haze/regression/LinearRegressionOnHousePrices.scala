package org.haze.regression

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorSlicer}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LinearRegressionOnHousePrices extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/house_prices"

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("LinearRegressionOnHousePrices'")
    .getOrCreate()


  val df = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(s"$base_data_dir/train.csv")

  import spark.implicits._

  df.createOrReplaceTempView("houses")

  //Store the target column in a val <- the column whose value to be predicted
  val labelField = "SalePrice"

   /* **********************************************
    * ########## Feature Transformation ############
    * **********************************************/

  /**
    * Categorical features transformation - Transform from String to Numerical/Scalar
    */

  // Extract all columns except "Id" and the to-be-predicted one "SalePrice"
  // Extract all fieldNames whose values are of Int
  val scalarFields: Seq[String] = df.schema.fields.collect {
    case StructField(name, IntegerType, _, _) if name != labelField && name != "Id" => name
  }
  val scalarData = df.map { row =>
    //Tuple2
    (
      Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
      row.getInt(row.fieldIndex(labelField))
    )
  }.toDF("scalar_features", labelField)

  scalarData.show

  // ToDo - Beside the Assembler, use StringIndexer to categorize String valued columns
  /*val inputCols: Array[String] = df.schema.fieldNames
    .filterNot(_.equalsIgnoreCase("name"))
    .filterNot(_.equalsIgnoreCase("id"))
  val assembler = new VectorAssembler()
    .setInputCols(inputCols)
    .setOutputCol("features")

  assembler.transform(df).show(40)*/

  //  Retrieving the StringTyped columns
  val stringFields: Seq[String] = df.schema.fields.collect {

    case StructField(name, StringType, _, _) => name

  }

  /**
    * Transforming all categorical columns using StringIndexer
    * Create a unique value/index for the categorical values such as 1StoryBuilding, 2StoryBuilding, etc
    */
  val categoricalDF = stringFields.foldLeft(df) { (dfAcc, nxtCol) =>
    val indexer = new StringIndexer() // StringIndexer Transformer
      .setInputCol(nxtCol)
      .setOutputCol(s"${nxtCol}_id")

    indexer.fit(dfAcc).transform(dfAcc)
  }
  
  //Test categorical columns with "HouseStyle"
  val House_Style_SQL = categoricalDF.select("HouseStyle", "HouseStyle_id").distinct()
  House_Style_SQL.show


  val scalar_categorical_ds = categoricalDF.map { row =>
    //Tuple3
    (
      //ScalaFields
      Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray), //Vectors.dense() accept only Double
      //StringFields which were transformed into Scalar ones
      Vectors.dense(stringFields.map(name => row.getAs[Double](s"${name}_id").toDouble).toArray),
      //
      row.getInt(row.fieldIndex(labelField))

    )

  }

  val scalar_categorical_df = scalar_categorical_ds.toDF("features_scalar", "features_categorical", labelField) // Renaming _.1, _.2, _.3, respectively
  scalar_categorical_df.show


   /* *****************************************************
    * ########## Feature Normalization/Scaling ############
    * *****************************************************/


  /**
    * Scaling Scalar Feature <- Normalization??
    * Via Standard Deviation & Mean <- Review the material from @ StanfordUniversity
    *
    * Observation: Categorical values need not be scaled because they're evenly spread out?
    */

  val stdScaler = new StandardScaler()
    .setInputCol("features_scalar")
    .setOutputCol("features_scalar_scaled")
    .setWithStd(true)
    .setWithMean(true)

  val scaledDataDF = stdScaler.fit(scalar_categorical_df).transform(scalar_categorical_df)
  scaledDataDF.show

  /**
    * Combining Data: Categorical + Scalar
    */

  val dataCombinedDS = scaledDataDF.map { row =>

    //concatenate two dense vectors
    val combinedFeatures = Vectors.dense(
      row.getAs[DenseVector]("features_scalar_scaled").values ++
        row.getAs[DenseVector]("features_categorical").values)

    (combinedFeatures, row.getAs[Int](labelField)) //Returning Tuple2

  }
  val dataCombinedDF = dataCombinedDS.toDF("features", labelField)
  dataCombinedDF.show


   /* ***************************************************
    * ########## SparkML - Feature Selectors ############
    * ***************************************************/

  /**
    * ToDo - Not needed here! Place this in another src code
    * Notice the "(1 until scalarFields.size)" is skipping the first index which the ID in the features table
    */
  /*val slicer = new VectorSlicer() // This is a Selector
    .setInputCol("scalar_features")
    .setOutputCol("features")
    .setIndices((1 until scalarFields.size).toArray)

  val output = slicer.transform(scalarData)

  output.show*/


   /* *************************************************************
    * ########## Data Splitting : Training & Test Sets ############
    * *************************************************************/

  /**
    * Modeling and Evaluation - Common Logic
    */

  // extract trainData & testData using Extractors
  val Array(trainData, testData) = dataCombinedDF.randomSplit(Array(0.7, 0.3)) // of ratio 70:30
  trainData.show
  testData.show


   /* *****************************************
    * ########## Linear Regression ############
    * *****************************************/


  // Instantiate the LinearRegression instance
  val lr = new LinearRegression()
    .setLabelCol(labelField)
    .setMaxIter(1000)
    .setRegParam(0.3) // ToDo - is this the LearningRate/StepSize? Theta?


   /* *******************************************
    * ########## Fit/Train the Model ############
    * *******************************************/

  // Fit the Model
  val lrModel = lr.fit(trainData) // ML Training and Fitting happens here!

   /* *******************************************
    * ########## Evaluating the Model ###########
    * *******************************************/

  // Evaluation
  val (trainLrPredictions, trainLrR2) = evaluate(trainData, lrModel)
  val (testLrPredictions, testLrR2) = evaluate(testData, lrModel)

  def evaluate(df: DataFrame, lrModel: Transformer): (DataFrame, Double) = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelField)
      .setPredictionCol("prediction")
      .setMetricName("r2") // R-Squared
    val predictions = lrModel.transform(df)
    val r2 = evaluator.evaluate(predictions)

    (predictions, r2)

  }


   /* **********************************************************
    * ########## Compare Predicted vs Actual Values ############
    * **********************************************************/

  // Output
  println(s"r2 on train data: $trainLrR2")
  println(s"r2 on test data: $testLrR2")

  val tableOutput = testLrPredictions.select($"prediction", $"${labelField}").show(10)

  // z.show(tableOutput)
}
