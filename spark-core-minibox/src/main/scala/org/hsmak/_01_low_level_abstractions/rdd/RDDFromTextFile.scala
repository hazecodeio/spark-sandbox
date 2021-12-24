package org.hsmak._01_low_level_abstractions.rdd

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Observations:
 *     - sc.textFile(): gives out RDD[String] <- not RDD[Row]
 *     - Ultimately you want to convert RDD to the a high level abstraction of DF/DS
 */
object RDDFromTextFile extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("RDDFromTextFile")
    .master("local[*]")
    .getOrCreate()

  println("--------------------- RDDFromCSVTextFile -----------------------")
  RDDFromCSVTextFile(spark)


  /**
   *
   * @param spark
   */
  def RDDFromCSVTextFile(spark: SparkSession) = {
    val sc = spark.sparkContext;

    import spark.implicits._
    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"
    val rddOfTextFile = sc.textFile(filePath) // Notice the use of SparkContext not SparkSession to emit RDD not DF/DS

    val rddOfWordGilbertCount: RDD[(String, Int)] = rddOfTextFile.flatMap(line => line.split(","))
      .filter(_ contains "Gilbert")
      .map(w => (w, 1)) // Mapping to a Tuple2 will yield a Key-Value HashMap; Hence we can use reduceByKey() as follows
      .reduceByKey { (a, b) => a + b }
      .sortByKey(true, 4)

    val dfFromRDD = rddOfWordGilbertCount.toDF("words", "frequency")
    dfFromRDD.show


    //ToDo - Move to conversions from RDD to DF/DS
    //parallel RDD
    /*val parallelRDD = sc.parallelize(rddOfWordGilbertCount.collect, 10)
    val dfFromParRDD = parallelRDD.toDF("words", "frequency")
    dfFromParRDD.show*/

    //ToDo - Move to conversions from DF/DS to RDD
    /*spark.read.option("header", true).option("inferSchema", true).csv(rddOfTextFile.toDS()).rdd.map{
      r => r.getAs[String]("Neighborhood")
    }.filter(_ equalsIgnoreCase  "Gilbert")
      .map(w => (w, 1)) // Mapping to a Tuple2 will yield a Key-Value HashMap; Hence we can use reduceByKey() as follows
      .reduceByKey { (a, b) => a + b }
      .sortByKey(true, 4)
      .toDF("words", "frequency").show*/
  }

}
