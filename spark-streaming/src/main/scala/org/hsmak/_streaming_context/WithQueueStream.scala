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
object WithQueueStream extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  val ssc = new StreamingContext(spark.sparkContext, Milliseconds(1000))

  val rdd = sc.emptyRDD[(Int, Int)]
  var queueOfRDDs = mutable.Queue[RDD[(Int, Int)]](rdd) // Queue needs to be at reach of the Caller/User

  val inDS: InputDStream[(Int, Int)] = ssc.queueStream(queueOfRDDs)
  //ToDo - How to do stateful streaming where aggregation and joining is possible?
  inDS.foreachRDD(r => r.toDF("k", "v").show()) // This is where the action is, right before start()
  ssc.start()


  while (true) {
    Thread.sleep(100)
    val _data = LazyList.continually((Random.nextInt(3), Random.nextInt(10))).take(10)
    val rdd: RDD[(Int, Int)] = sc.parallelize(_data)
    queueOfRDDs.enqueue(rdd)
  }
  ssc.awaitTermination()

}
