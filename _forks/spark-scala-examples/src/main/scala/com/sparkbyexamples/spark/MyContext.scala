package com.sparkbyexamples.spark

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

trait MyContext {
  Configurator.setLevel("org.apache.spark", Level.OFF)
  val base_dir = s"file://${System.getProperty("user.dir")}"
  val data_dir = base_dir + "/_forks/spark-scala-examples/src/main/resources"
  val out_dir = base_dir + "/_data/OUT"
}
