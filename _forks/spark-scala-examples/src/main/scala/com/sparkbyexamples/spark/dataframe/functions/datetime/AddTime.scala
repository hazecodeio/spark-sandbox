package com.sparkbyexamples.spark.dataframe.functions.datetime

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
object AddTime extends App with MyContext {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  spark.sql( "select current_timestamp," +
    "cast(current_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours_of_2," +
    "cast(current_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes_of_5," +
    "cast(current_timestamp as TIMESTAMP) + INTERVAL 55 seconds as added_seconds_of_55"
  ).show(false)


  val df = Seq(
    "2019-07-01 12:01:19.101",
    "2019-06-24 12:01:19.222",
    "2019-11-16 16:44:55.406",
    "2019-11-16 16:50:59.406")
    .toDF("input_timestamp")


  df.createOrReplaceTempView("AddTimeExample")

  val df2 = spark.sql(
  """select input_timestamp,
     | cast(input_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours_of_2,
     | cast(input_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes_of_5,
     | cast(input_timestamp as TIMESTAMP) + INTERVAL 55 seconds as added_seconds_of_55
     | from AddTimeExample""".stripMargin)
  df2.show(false)

  df.withColumn("added_hours_of_2",col("input_timestamp") + expr("INTERVAL 2 HOURS"))
    .withColumn("added_minutes_of_2",col("input_timestamp") + expr("INTERVAL 2 minutes"))
    .withColumn("added_seconds_of_2",col("input_timestamp") + expr("INTERVAL 2 seconds"))
    .show(false)
}
