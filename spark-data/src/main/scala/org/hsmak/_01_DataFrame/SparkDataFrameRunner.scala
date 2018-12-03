package org.hsmak._01_DataFrame

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object SparkDataFrameRunner {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("SparkDataFrameRunner")
      .getOrCreate()

    import spark.implicits._



    spark.stop()
  }

}