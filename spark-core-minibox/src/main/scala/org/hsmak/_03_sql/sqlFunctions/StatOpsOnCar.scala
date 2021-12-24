package org.hsmak._03_sql.sqlFunctions

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object StatOpsOnCar extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StatOpsOnCar")
    .getOrCreate()


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

  val cor = carMileageDF.stat.corr("hp","weight")
  println("hp to weight : Correlation = %.4f".format(cor))
  
  val cov = carMileageDF.stat.cov("hp","weight")
  println("hp to weight : Covariance = %.4f".format(cov))

  //TODO - Understand how the `CrossTabulation` function provides a table of the frequency distribution for a set of variables!
  carMileageDF.stat.crosstab("automatic","NoOfSpeed").show()
}
