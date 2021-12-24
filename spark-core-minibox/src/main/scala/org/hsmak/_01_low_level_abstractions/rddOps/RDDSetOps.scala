package org.hsmak._01_low_level_abstractions.rddOps

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDSetOps {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDDSetOps")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    spark.stop()
  }

}
