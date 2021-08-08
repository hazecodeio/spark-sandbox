package org.hsmak._streaming_context

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable

/**
  * @author ${user.name}
  */
object WithQueueStream extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  val ssc = new StreamingContext(spark.sparkContext, Milliseconds(500))

  val rdd = sc.emptyRDD[Int]
  var queueOfRDDs = mutable.Queue[RDD[Int]](rdd) // Queue needs to be at reach of the Caller/User

  val inDS: InputDStream[Int] = ssc.queueStream(queueOfRDDs)
  inDS.foreachRDD(r => r.toDF().show()) // This is where the action is, right before start()
  ssc.start()


  while (true) {
    Thread.sleep(100)
    val _data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val rdd: RDD[Int] = sc.parallelize(_data)
    queueOfRDDs.enqueue(rdd)
  }
  ssc.awaitTermination()

}
