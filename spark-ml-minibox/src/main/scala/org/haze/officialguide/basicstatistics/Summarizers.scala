package org.haze.officialguide.basicstatistics

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession


/**
  * The following example demonstrates using Summarizer to compute the
  *   - mean and
  *   - variance
  * for a vector column of the input dataframe, with and without a weight column.
  */
object Summarizers extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Summarizers")
    .getOrCreate()


  val data = Seq(
    (Vectors.dense(2.0, 3.0, 5.0), 1.0),
    (Vectors.dense(4.0, 6.0, 7.0), 2.0)
  )

  import spark.implicits._

  val df = data.toDF("features", "weight")
  df.show

  //  import org.apache.spark.sql.functions._
  import org.apache.spark.ml.stat.Summarizer._ // import metrics()

  val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
    .summary($"features", $"weight").as("summary"))
    .select("summary.mean", "summary.variance")
    .as[(Vector, Vector)].first()

  println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

  val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
    .as[(Vector, Vector)].first()

  println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")


}
