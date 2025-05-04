package org.haze._03_sql.sqlFunctions

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object ScientificOpsOnHypot extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("ScientificOpsOnHypot")
    .getOrCreate()


   /* *****************************************************
    * ############ Creating RDD from Collection ###########
    * *****************************************************/


  val aList: List[Int] = List(-1, 10, 100, 1000)
  var aRDD = spark.sparkContext.parallelize(aList) // RDD[Int]

  /**
    * `import spark.implicits._` can replace the following two line.
    * Note: you will have to call toDS() instead of createDataset() and some other nuances
    */
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._ //import RDD - DS implicit converters; and the $ notation

  val ds = spark.createDataset(aRDD)
  //  val ds = aRDD.toDS // alternative to the previous line
  ds.show()

  import org.apache.spark.sql.functions.{log, log10, sqrt}

  ds.select(ds("value"), log(ds("value")).as("ln")).show
  ds.select(ds("value"), log10(ds("value")).as("log10")).show
  ds.select(ds("value"), sqrt(ds("value")).as("sqrt")).show

  //  import spark.implicits._ // $-notation is already imported from 'sqlContext' above

  // or in one line and using the $ notation
  ds.select($"value", log($"value").as("ln"), log10($"value").as("log10"), sqrt($"value").as("sqrt")).show



   /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val hypotDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/hypot.csv")


  println("Data has " + hypotDF.count() + " rows")
  hypotDF.show(5)
  hypotDF.printSchema()

  import org.apache.spark.sql.functions.hypot

  /**
    * From Trigonometry: Z^2 = X^2 + Y^2; where Z is the hypotenuse.
    */
  hypotDF.select($"X", $"Y", hypot($"X", $"Y").as("hypot")).show
}
