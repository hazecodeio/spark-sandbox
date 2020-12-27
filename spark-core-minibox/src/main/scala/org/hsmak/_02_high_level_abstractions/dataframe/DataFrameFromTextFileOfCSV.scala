package org.hsmak._02_high_level_abstractions.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{SparkSession, functions}

/**
  * @author ${user.name}
  */
object DataFrameFromTextFileOfCSV {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("DatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"

    val df = spark.read.option("header", true).option("inferSchema", true).csv(filePath)
//    df.show(1)

    // Notice the use of SparkSession not SparkContext to emit DF/DS not RDD
    val dfTransformedDS = df
      .map(row => row.getAs[String]("Neighborhood"))
      .map(w => (w, 1)) // emits Dataset

    dfTransformedDS.show()

    val df2 = dfTransformedDS.toDF("words", "frequency") //back to DF
      .groupBy("words")
      .agg(functions.count("frequency") as "freq_count")
      .orderBy(desc("freq_count"))
    df2.show()

    spark.stop()
  }
}