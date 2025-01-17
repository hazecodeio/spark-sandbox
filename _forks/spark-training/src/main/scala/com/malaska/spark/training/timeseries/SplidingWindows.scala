package com.malaska.spark.training.timeseries

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object SplidingWindows {
  Configurator.setLevel("org.apache.spark", Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val leadLagJson = args(0)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .enableHiveSupport()
        .getOrCreate()
    }
    println("---")

    import sparkSession.implicits._

    val leadLag = sparkSession.read.json(leadLagJson).as[JsonLeadLag]

    leadLag.createOrReplaceTempView("leadlag")

    sparkSession.sql("select * from leadlag").collect().foreach(println)

    val leadLagDf = sparkSession.sql("SELECT " +
      "group, ts, " +
      "value as v_now, " +
      "AVG(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg, " +
      "Min(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg, " +
      "Max(value) OVER (ORDER BY ts rows between 3 preceding and current row) as v_moving_avg " +
      "FROM leadlag")

    leadLagDf.collect().foreach(println)

  }
}
