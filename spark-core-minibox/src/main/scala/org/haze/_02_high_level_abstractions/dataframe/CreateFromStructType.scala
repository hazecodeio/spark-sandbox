package org.haze._02_high_level_abstractions.dataframe

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}

object CreateFromStructType extends App {


  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("RDDFromCollections")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext


  println("--------------------- createSchema - simple ---------------------")
  object createSchemaSimple {
    // One record of 30 columns
    val values = Seq(1, "Scala")
    val row = Row.fromSeq(values)

    //Schema
    val schema: StructType = StructType(Seq(
      StructField(name = "id", dataType = IntegerType, nullable = false),
      StructField(name = "name", dataType = StringType, nullable = false)
    ))

    spark.createDataFrame(sc.parallelize(Seq(row, row)), schema).show()
    spark.createDataFrame(sc.makeRDD(Seq(row, row)), schema).show() // it calls parallelize() under the hood
  }

  createSchemaSimple

  println("--------------------- createSchema - Dynamically ---------------------")

  object createSchemaDynamically {
    // One record of 30 columns
    val values = (1 to 30)
    val row = Row.fromSeq(values)

    //Schema
    val schema: StructType = StructType((1 to 30).map(i => // creating 30 columns
      StructField(name = s"col$i", dataType = IntegerType, nullable = false)
    ))

    spark.createDataFrame(sc.parallelize(Seq(row, row)), schema).show()
    spark.createDataFrame(sc.makeRDD(Seq(row, row)), schema).show()
  }

  createSchemaDynamically

  /**
    * Another way of creating a StructType/Schema
    *
    */
  println("--------------------- createSchemaFromCaseClass ---------------------")

  object createSchemaFromCaseClass {

    val schema2: StructType = Encoders.product[Emp].schema;

    case class Emp(id: Int, name: String)

    println(schema2)
  }

  createSchemaFromCaseClass

}
