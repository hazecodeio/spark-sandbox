package org.haze._03_sql.sqlFunctions

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

object CrossTabOpsOnTitanic extends App {

  //turn off Logging
  Configurator.setLevel("org.apache.spark", Level.OFF)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/titanic"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("CrossTabOpsOnTitanic")
    .getOrCreate()


   /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val titanicPassengersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/titanic3_02.csv")


  println("titanicPassengersDF has " + titanicPassengersDF.count() + " rows")
  titanicPassengersDF.show(5)
  titanicPassengersDF.printSchema()


  val passengersQuery = titanicPassengersDF.select(
    titanicPassengersDF("Pclass"),
    titanicPassengersDF("Survived"),
    titanicPassengersDF("Gender"),
    titanicPassengersDF("Age"),
    titanicPassengersDF("SibSp"),
    titanicPassengersDF("Parch"),
    titanicPassengersDF("Fare"))

  passengersQuery.show(5)
  passengersQuery.printSchema()


  passengersQuery.groupBy("Gender").count().show()

  //How many of each gender survived?
  passengersQuery.stat.crosstab("Survived", "Gender").show()

  //Did passengers traveling with spouses/siblings have a better chance of survival?
  passengersQuery.stat.crosstab("Survived", "SibSp").show()

  // Did age make any difference?
  //passengersQuery.stat.crosstab("Survived","Age").show() // Create AgeBrackets/AgeRanges instead as in the following

  /**
    * AgeBracket:
    * (age - age % 10): create an AgeBracket of 10
    *
    */
  val ageDist = passengersQuery.select(
    passengersQuery("Survived"),
    (passengersQuery("age") - passengersQuery("age") % 10).cast("int").as("AgeBracket"))

  ageDist.show(3)
  ageDist.stat.crosstab("Survived", "AgeBracket").show()


   /* *******************************************
    * ########## Using the $ Notation ###########
    * *******************************************/

  val sqlContext = spark.sqlContext
  //import the $ notation
  import sqlContext.implicits._
  //  import spark.implicits._ // alternative to the previous line

  val ageDist$ = passengersQuery.select(
    $"Survived",
    ($"age" - $"age" % 10).cast("int").as("AgeBracket"))

  ageDist$.show(3)
  ageDist$.stat.crosstab("Survived", "AgeBracket").show()

}
