package org.hsmak._02_high_level_abstractions.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreateFromURL extends App {


  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("RDDFromTextFile")
    .master("local[*]")
    .getOrCreate()

  println("--------------------- RDDOpsFromJsonTextFile -----------------------")
  RDDOpsFromJsonTextFile(spark)

  /**
    * Observations:
    *   - It's really converting the content from the URL into a Multiline String, then
    *   - Convert into a Seq[String]; where the huge String is split by the newline '\n'
    *   - Parallelize the Seq[String]; this would generate an RDD[String], then
    *   - Convert RDD[String] to Dataset[String], then
    *   - Pass the Dataset[String] to spark.read.json()
    *
    * @param spark
    */
  def RDDOpsFromJsonTextFile(spark: SparkSession) = {
    val sc = spark.sparkContext
    import spark.implicits._

    // TODO - to be tested
    import scala.io.Source._

    val url = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2010-summary.json#"
    // read a json file from a url stream
    val jsonArrStr = fromURL(url).mkString.split("\n") // since it's a multi-line json

    // convert Json string to RDD, since we're not reading from File
    val rddJson = sc.parallelize(jsonArrStr, 8)

    //convert RDD -> DS -> DF
    val df = spark.read.option("header", true).option("inferSchema", true).json(rddJson.toDS)

    // Via SparkSQL API
    import org.apache.spark.sql.functions._
    df.groupBy($"DEST_COUNTRY_NAME").agg(sum($"count") as "sum").select("*").sort($"sum".desc).show

    // via SQL query
    df.createOrReplaceTempView("flights")
    spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as sum FROM flights GROUP BY DEST_COUNTRY_NAME ORDER BY sum DESC LIMIT 5").show
  }

}
