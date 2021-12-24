package org.hsmak._01_low_level_abstractions

import au.com.bytecode.opencsv.CSVReader
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

import java.io.StringReader

object SharedStateWithAccumulator {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("AccumulatorRunner")
      .getOrCreate()

    val sc = spark.sparkContext

    println(s"Running Spark Version ${sc.version}")

    //Deprecated
    //    val invalidLineCounter = sc.accumulator(0);
    //    val invalidNumericLineCounter = sc.accumulator(0);

    val invalidLineCounter = sc.longAccumulator("invalidLineCounter")
    val invalidNumericLineCounter = sc.longAccumulator("invalidNumericLineCounter")

    val filePath = s"file://${System.getProperty("user.dir")}/_data/line-of-numbers.csv"
    val txtFileRDD = sc.textFile(filePath) // RDD[String]

    val splitLines = txtFileRDD.flatMap(line => {
      try {
        val reader = new CSVReader(new StringReader(line))
        Some(reader.readNext())
      } catch {
        case _ => {
          invalidLineCounter.add(1)
          None
        }
      }
    })

    val numericData = splitLines.flatMap(line => {
      try {
        Some(line.map(_.toDouble))
      } catch {
        case _ => {
          invalidNumericLineCounter.add(1)
          None
        }
      }
    })

    val summedData = numericData.map(row => row.sum)

    println(summedData.collect().mkString(","))
    println(
      s"""Errors:
         |--> ${invalidLineCounter}
         |--> ${invalidNumericLineCounter}""".stripMargin)
  }
}
