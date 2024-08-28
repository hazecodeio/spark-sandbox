package org.hsmak._streaming_context

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

/**
  * @author ${user.name}
  */
object WithQueueStream2 extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Link https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  // Run NetCat to listen to the stream -> $nc -lk 9999

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()



  val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

  query.awaitTermination()


}
