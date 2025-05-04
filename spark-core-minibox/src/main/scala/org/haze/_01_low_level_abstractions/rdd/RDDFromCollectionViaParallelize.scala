package org.haze._01_low_level_abstractions.rdd

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Observations on creating RDD from Collections:
  *     - Always via "sc.parallelize(seq())"
  *     - A Seq records of one column
  *     - To represent multiple columns, Tuples need to be used
  *         - sc.parallelize(Seq((1, "11"),(2, "22"), ...))
  *     - Can't seem to parallelize a Seq of Seq
  *         - That's when DataFrames/DataSets come in the picture
  *         - SchemaType/StructType would be involved
  */
object RDDFromCollectionViaParallelize extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("RDDFromCollections")
    .master("local[*]")
    .getOrCreate()

  rddOfOneColumn(spark)
  rddOfMultipleColumnsViaTuple(spark)
  RDDFromString("ababajshgsdjsahd", spark) // String needs to be transformed to Seq[Char]

  runAsFutures(
    () => {
      rddOfOneColumn(spark)
    },
    () => {
      rddOfMultipleColumnsViaTuple(spark)
    },
    () => {
      RDDFromString("ababajshgsdjsahd", spark)
    })

  // of multiple columns via Tuples <- max of Tuple22 (as of scala 2.12)
  def rddOfMultipleColumnsViaTuple(spark: SparkSession) = {
    println("---------------------- rddOfMultipleColumnsViaTuple ----------------------")

    val sc = spark.sparkContext;
    import spark.implicits._

    val rddOfMultipleColsViaTuple: RDD[(String, Int)] = sc.parallelize(
      Seq(
        ("scala", 1111),
        ("Java", 2222),
        ("Python", 3333)))
    rddOfMultipleColsViaTuple.collect().foreach(println)
    rddOfMultipleColsViaTuple.toDF.show
  }

  def RDDFromString(str: String, spark: SparkSession) = {
    println("---------------------- RDDFromString ----------------------")

    val sc = spark.sparkContext
    import spark.implicits._

    val rddOfString = sc.parallelize(str.map(c => c.toString))
    val rddOfCharCounts = rddOfString.map((_, 1)).reduceByKey(_ + _)
    val dfOfCharCounts = rddOfCharCounts.toDF("char", "count")
    dfOfCharCounts.show
  }

  // of one column
  def rddOfOneColumn(spark: SparkSession) = {
    println("---------------------- rddOfOneColumn ----------------------")

    val sc = spark.sparkContext;
    import spark.implicits._

    val rddOfOneCol: RDD[String] = sc.parallelize(
      Seq(
        "Scala",
        "Java",
        "Python"))
    rddOfOneCol.collect().foreach(println)
    rddOfOneCol.toDF.show
  }

  def runAsFutures(blocks: () => Unit*) = {
    println("---------------------- runAsFutures ----------------------")

    import scala.concurrent.ExecutionContext.Implicits._

    val futures = blocks.map(f => Future(f()))
      .toArray

    futures.map(f => Await.ready(f, Duration.Inf))
  }
}