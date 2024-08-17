package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object JsonFromMultiline extends App {

    val spark: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("SparkByExamples.com")
        .getOrCreate()

    val base_data_dir = s"file://${System.getProperty("user.dir")}/_forks/spark-scala-examples/"

    //read multiline json file
    val multiline_df = spark.read.option("multiline", "true")
        .json(s"$base_data_dir/src/main/resources/multiline-zipcode.json")
    multiline_df.printSchema()
    multiline_df.show(false)

}
