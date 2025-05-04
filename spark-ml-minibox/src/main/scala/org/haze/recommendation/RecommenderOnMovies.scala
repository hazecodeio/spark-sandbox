package org.haze.recommendation

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}

object RecommenderOnMovies extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/movielens"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("RecommenderOnMovies")
    .getOrCreate()


  import spark.implicits._


   /* ******************************************************
    * ############ Loading Data & Transformation ###########
    * ******************************************************/


  val movies = spark.read
    .text(s"${base_data_dir}/movies.dat")
    .map { row =>

      val cols = row.getString(0).split("::")

      (cols(0).toInt, cols(1), cols(2)) //Tuple

    }.toDF("MovieID", "Title", "Genres")
  movies.show(5, truncate = false)
  movies.printSchema()

  val ratings = spark.read
    .text(s"${base_data_dir}/ratings.dat")
    .map { row =>

      val cols = row.getString(0).split("::")

      (cols(0).toInt, cols(1).toInt, cols(2).toDouble, cols(3).toLong)

    }.toDF("UserID", "MovieID", "Rating", "Timestamp")
  ratings.show(5, truncate = false)
  ratings.printSchema

  val users = spark.read
    .text(s"${base_data_dir}/users.dat")
    .map { row =>

      val cols = row.getString(0).split("::")

      (cols(0).toInt, cols(1), cols(2).toInt, cols(3).toInt, cols(4))

    }.toDF("UserID", "Gender", "Age", "Occupation", "zip-code")
  users.show(5, truncate = false)
  users.printSchema

  println("Got %d ratings from %d users on %d movies.".format(ratings.count(), users.count(), movies.count()))

  // Split data

  val Array(train, test) = ratings.randomSplit(Array(0.8, 0.2))
  println("Train = " + train.count() + " Test = " + test.count())

  // Train the ALS model

  val algALS = new ALS()
    .setUserCol("UserID") // defaulting to user
    .setItemCol("MovieID") // defaulting to item
    .setRatingCol("Rating") // defaulting to rating
    .setRank(12)
    .setRegParam(0.1)
    .setMaxIter(20)
  val mdlReco = algALS.fit(train)

  // use the model to predict on the testSet

  val predictions = mdlReco.transform(test)
  predictions.show(5)
  predictions.printSchema()

  val pred = predictions.na.drop()
  println(s"Orig = ${predictions.count()} Final = ${pred.count()} Dropped = ${(predictions.count() - pred.count())}")

  // Calculate RMSE & MSE
  val evaluator = new RegressionEvaluator()
  evaluator.setLabelCol("Rating")

  var rmse = evaluator.evaluate(pred)
  println("Root Mean Squared Error = " + "%.3f".format(rmse))

  evaluator.setMetricName("mse")

  var mse = evaluator.evaluate(pred)
  println("Mean Squared Error = " + "%.3f".format(mse))

  mse = (pred.rdd.map(row => calcMSE(row)).reduce(_ + _)) / predictions.count().toDouble
  println("Mean Squared Error (Calculated) = " + "%.3f".format(mse))


  /**
    * helper method to calculate the MeanSquaredError by hand
    *
    * @param row
    * @return
    */
  def calcMSE(row: Row): Double = {
    math.pow((row.getDouble(2) - row.getFloat(4).toDouble), 2)
  }
}
