package org.haze._01_low_level_abstractions.example

import au.com.bytecode.opencsv.CSVReader
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.StringReader

/**
  * @author ${user.name}
  */
object ReadWriteLineNumbers {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDDWithCSV")
      .getOrCreate()

    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"
    val lonRDD = sc.textFile(s"${base_data_dir}/line-of-numbers.csv")

    val splitLinesRDD = lonRDD.map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })

    val numericDataRDD = splitLinesRDD.map(line => line.map(_.toDouble))
    val summedDataRDD = numericDataRDD.map(row => row.sum)

    println(summedDataRDD.collect().mkString(","))

    // Writing summation result into a CSV file
    import spark.implicits._
    summedDataRDD.toDF("Value")
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"${base_data_dir}/out-line-of-numbers")
    spark.stop()
  }

}
