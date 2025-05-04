package org.haze.regression

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DecisionTreeRegressionOnHousePrices extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/house_prices"

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DecisionTreeRegressionOnHousePrices'")
    .getOrCreate()


  val df = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(s"$base_data_dir/train.csv")

  import spark.implicits._


  // Extracting Fields/Columns

  //Store the target column in a val <- the column whose value to be predicted
  val labelField = "SalePrice"


  // Extract all columns except "Id" and the to-be-predicted one "SalePrice"
  val scalarFields: Seq[String] = df.schema.fields.collect { case StructField(name, IntegerType, _, _) if name != labelField && name != "Id" => name }

  // this time we're retreiving StringTyped columns
  val stringFields: Seq[String] = df.schema.fields.collect { case StructField(name, StringType, _, _) => name }


  //Extracting Scalar & Categorical Features into Vectors


  /**
    * Create a unique value/index for the categorial values such as 1StoryBuilding, 2StoryBuilding, etc
    */
  val dfWithCategories = stringFields.foldLeft(df) { (dfTemp, nxtCol) =>
    val indexer = new StringIndexer()
      .setInputCol(nxtCol)
      .setOutputCol(s"${nxtCol}_id")

    indexer.fit(dfTemp).transform(dfTemp)
  }

  val data = dfWithCategories.map { row =>
    //Tuple3
    (
      //ScalaFields
      Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
      //StringFields which was transformed into Scalar ones
      Vectors.dense(stringFields.map(name => row.getAs[Double](s"${name}_id").toDouble).toArray),
      row.getInt(row.fieldIndex(labelField))

    )

  }.toDF("features_scalar", "features_categorical", "label").limit(100) // Renaming _.1, _.2, _.3, respectively

  data.show


  ///////////////////////////////


  val stdScaler = new StandardScaler()
    .setInputCol("features_scalar")
    .setOutputCol("features_scalar_scaled")
    .setWithStd(true)
    .setWithMean(true)

  val data_scaled = stdScaler.fit(data).transform(data)
  // z.show(data_scaled)

  val dataCombined = data_scaled.map { row =>

    val combinedFeatures = Vectors.dense(row.getAs[DenseVector]("features_scalar_scaled").values ++
      row.getAs[DenseVector]("features_categorical").values)

    (combinedFeatures, row.getAs[Int]("label")) //Returning Tuple2

  }.toDF("features", "label")

  dataCombined.show


  //////////////////////////


  // extract trainData & testData using Extractors
  val Array(trainData, testData) = dataCombined.randomSplit(Array(0.7, 0.3))

  def evaluate(ds: DataFrame, model: Transformer): (DataFrame, Double) = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val predictions = model.transform(ds)
    val r2 = evaluator.evaluate(predictions)

    (predictions, r2)

  }


  // Instantiate the DecisionTreeRegressor instance
  val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setMaxBins(1000)
    .setMaxDepth(10)

  // Fit the Model
  val dtModel = dt.fit(trainData) // ML Training and Fitting happens here!

  // Evaluation
  val (trainLrPredictions, trainDtR2) = evaluate(trainData, dtModel)
  val (testLrPredictions, testDtR2) = evaluate(testData, dtModel)

  // Output
  println(s"r2 on train data: $trainDtR2")
  println(s"r2 on test data: $testDtR2")

  val tableOutput = testLrPredictions.select($"prediction", $"label").show(10)

}
