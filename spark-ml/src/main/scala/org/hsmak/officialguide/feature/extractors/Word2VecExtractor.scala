package org.hsmak.officialguide.feature.extractors

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

object Word2VecExtractor extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Word2VecExtractor")
    .getOrCreate()


  // Input data: Each row is a bag of words from a sentence or document.
  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")
  documentDF.show

  // Learn a mapping from words to Vectors.
  val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(3)
    .setMinCount(0)
  val model = word2Vec.fit(documentDF)

  val result = model.transform(documentDF)
  result.collect().foreach {
    case Row(text: Seq[_], features: Vector) => println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
  }

}
