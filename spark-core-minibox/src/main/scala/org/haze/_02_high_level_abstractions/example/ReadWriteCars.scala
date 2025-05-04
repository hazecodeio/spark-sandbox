package org.haze._02_high_level_abstractions.example

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadWriteCars extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"


  /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("DatasetRunner")
    .getOrCreate()


  /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val carMileageDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/cars.csv")


  println("carMileageDF has " + carMileageDF.count() + " rows")
  carMileageDF.show(5)
  carMileageDF.printSchema()


  /* ******************************************************
    * ############ Writing DataFrames back #################
    * ******************************************************/

  //Write DF back in CSV format
  carMileageDF.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv(s"${base_data_dir}/out/cars-out-csv")


  //Write DF back in Parquet format
  carMileageDF.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .partitionBy("year")
    .parquet(s"${base_data_dir}/out/cars-out-pqt")

}
