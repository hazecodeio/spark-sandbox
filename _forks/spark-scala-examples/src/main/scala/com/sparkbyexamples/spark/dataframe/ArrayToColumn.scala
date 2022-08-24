package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.functions.{max, size}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Observations:
 *     - Whenever "Row" is used to construct data rows, there must be a schema of StructType to be constructed alongside
 */
object ArrayToColumn extends App with MyContext {

  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayData = Seq(
    Row("James", List("Java", "Scala", "C++")),
    Row("Michael", List("Spark", "Java", "C++")),
    Row("Robert", List("CSharp", "VB", ""))
  )

  //  val arraySchema = new StructType().add("name",StringType)
  //    .add("subjects",ArrayType(StringType))

  // Alternative to the previous schema construction:
  val arraySchema = StructType(Seq(
    StructField("name", StringType),
    StructField("subjects", ArrayType(StringType))))

  val arrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
  arrayDF.printSchema()
  arrayDF.show()

  val arrayDFColumn = arrayDF.select(
    arrayDF("name") +: (0 until 2).map(i => arrayDF("subjects")(i).alias(s"LanguagesKnown$i")): _*
  )

  arrayDFColumn.show(false)

  //How to convert Array of Array to column
  val arrayArrayData = Seq(
    Row("James", List(List("Java", "Scala", "C++"), List("Spark", "Java"))),
    Row("Michael", List(List("Spark", "Java", "C++"), List("Spark", "Java"))),
    Row("Robert", List(List("CSharp", "VB"), List("Spark", "Python")))
  )

  val arrayArraySchema = new StructType().add("name", StringType)
    .add("subjects", ArrayType(ArrayType(StringType)))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData), arrayArraySchema)
  df.printSchema()
  df.show()

  /*
   * Observations:
   *    - Flatten records using Column operations of the DataFrame horizontally
   *    - Explode(), however, will flatten the elements vertically
   */
  val df2 = df.select(
    df("name") +: (0 until 2).map(i => df("subjects")(i).alias(s"LanguagesKnown$i")): _* // ':_*' is used to expand the resulting Array into repeated param colNames*
  )

  df2.show(false)

  /*
   * There's one problem about the previous way adding columns. Number of columns is hardcoded and NOT dynamic!!
   * Below is the way to do that.
   */

  import spark.implicits._
  val sc = spark.sparkContext
  object ArrayToColumnsDynamically {

    val numCols = df
      .withColumn("size", size($"subjects"))
      .agg(max($"size"))
      .head() // extract one Row; though there's only one row but easier than dealing with an array of one row
      .getInt(0) // get the first column of this row; there's only one column anyway

    val df2 = df.select(
      df("name") +: (0 until numCols).map(i => df("subjects")(i).alias(s"LanguagesKnown$i")): _* // ':_*' is used to expand the resulting Array into repeated param colNames*
    )

    df2.show(false)
  }
  ArrayToColumnsDynamically

  object ArrayToColumnsDynamically_02 {

    val df = sc.parallelize(Seq(Seq(1, 2), Seq(3, 4))).toDF("arr")
    val numCols = df
      .withColumn("size", size($"arr"))
      .agg(max($"size"))
      .head() // extract one Row; though there's only one row but easier than dealing with an array of one row
      .getInt(0) // get the first column of this row; there's only one column anyway

    df
      .select(
        $"*" +: (0 until numCols).map(i => $"arr".getItem(i).as(s"_c$i")): _*
      )
      .show()
  }

  ArrayToColumnsDynamically_02
}
