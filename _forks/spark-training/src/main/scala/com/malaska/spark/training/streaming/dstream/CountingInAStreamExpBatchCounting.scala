package com.malaska.spark.training.streaming.dstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountingInAStreamExpBatchCounting {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    val checkpointFolder = args(2)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host","127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .enableHiveSupport()
        .master("local[3]")
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .enableHiveSupport()
        .getOrCreate()
    }

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(2))
    ssc.checkpoint(checkpointFolder)

    val lines = ssc.socketTextStream(host, port.toInt)
    val words = lines.flatMap(line => line.toLowerCase.split(" "))
    val wordCounts = words.map(word => (word, 1))
      .reduceByKey((a,b) => a + b)

    wordCounts.foreachRDD(rdd => {
      println("{")
      val localCollection = rdd.collect()
      println("  size:" + localCollection.length)
      localCollection.foreach(r => println("  " + r))
      println("}")
    })

    ssc.start()

    ssc.awaitTermination()


  }
}
