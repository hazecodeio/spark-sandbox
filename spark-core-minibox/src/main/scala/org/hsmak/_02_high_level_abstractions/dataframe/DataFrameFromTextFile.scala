package org.hsmak._02_high_level_abstractions.dataframe

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object DataFrameFromTextFile {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("DatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"

    // Notice the use of SparkSession not SparkContext to emit DataSet[String] not RDD
    val stringDS = spark.read.textFile(filePath)

    val dataset = stringDS // Transformation ops will emit Dataset
      .flatMap(line => line.split(",")) // spark.read.csv() will take care of all this
      .map(w => (w, 1)) // why can't reduceByKey on Dataset???

    dataset.show()

    val df2 = dataset.toDF("words", "frequency")
    df2.show()

    spark.stop()
  }
}
