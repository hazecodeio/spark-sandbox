package org.haze.statistics

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object BasicStatistics extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("BasicStatistics")
    .getOrCreate()


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"

   /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val carMileageDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/car-mileage.csv")


  println("carMileageDF has " + carMileageDF.count() + " rows")
  carMileageDF.show(5)
  carMileageDF.printSchema()

  // Let us find summary statistics
  carMileageDF.describe("mpg", "hp", "weight", "automatic").show()

  // correlations
  var cor = carMileageDF.stat.corr("hp", "weight")
  println("hp to weight : Correlation = %2.4f".format(cor))

  var cov = carMileageDF.stat.cov("hp", "weight")
  println("hp to weight : Covariance = %2.4f".format(cov))

  cor = carMileageDF.stat.corr("RARatio", "width")
  println("Rear Axle Ratio to width : Correlation = %2.4f".format(cor))

  cov = carMileageDF.stat.cov("RARatio", "width")
  println("Rear Axle Ratio to width : Covariance = %2.4f".format(cov))

}
