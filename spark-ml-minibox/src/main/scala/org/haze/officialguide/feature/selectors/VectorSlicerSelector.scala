package org.haze.officialguide.feature.selectors

import java.util
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.util.stream.Stream

object VectorSlicerSelector extends App {


  Configurator.setLevel("org.apache.spark", Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("VectorSlicerSelector")
    .getOrCreate()

  viaOfficialGuide(spark)
  viaMe(spark)

  def viaOfficialGuide(spark:SparkSession) = {


    val data = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))
    dataset.show

    val slicer = new VectorSlicer()
      .setInputCol("userFeatures")
      .setOutputCol("features")

    slicer.setIndices(Array(1))
      .setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)
  }

  def viaMe(spark: SparkSession) = {
    Utils.printMethodNameViaStackWalker(1)
    val data = Seq(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    // ToDo - Refactor this into Schema of StructTypes
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val sc = spark.sparkContext;
    val dataset = spark.createDataFrame(sc.parallelize(data), // Notice the parallelize() on Seq
      StructType(Array(attrGroup.toStructField())))
    dataset.show

    val slicer = new VectorSlicer()
      .setInputCol("userFeatures")
      .setOutputCol("features")

    slicer.setIndices(Array(1))
      .setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)
  }
}

object Utils{
  def printMethodNameViaStackWalker(skip: Int): Unit = {
    val methodName = StackWalker.getInstance.walk((stream: Stream[StackWalker.StackFrame]) => stream.skip(skip).findFirst).get.getMethodName
    System.out.println(String.format("--- %s() ---", methodName))
  }
}