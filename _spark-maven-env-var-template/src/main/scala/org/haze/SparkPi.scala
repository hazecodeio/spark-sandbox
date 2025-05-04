package org.haze

import org.apache.spark.sql.SparkSession

import scala.math.random

/**
  * Run via maven (create as many jars for each AppRunner/MainClass): <- This is good only when there is one AppRunner/MainClass
  *   $ mvn clean compile package exec:exec@run-local
  *
  *
  * Run via spark-submit (you will need to supply the path to the AppRunner/MainClass):
  *   $ spark-submit --class org.haze.SparkPi --master local[2] spark-core-1.0-SNAPSHOT.jar 10
  *
  */

/** Computes an approximation to pi */
object SparkPi {


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      //.master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("Spark Pi")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2

    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow

    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * count / (n - 1)}")

    spark.stop()
  }

}
