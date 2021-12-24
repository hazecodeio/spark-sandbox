package org.hsmak._02_high_level_abstractions.dataframe

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{SparkSession, functions}

/**
  * @author ${user.name}
  */
object DataFrameFromTextFileOfCSV {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("DatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"

    val df = spark.read.option("header", true).option("inferSchema", true).csv(filePath)
//    df.show(1)

    // Notice the use of SparkSession not SparkContext to emit DataFrame[Row] not RDD
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
