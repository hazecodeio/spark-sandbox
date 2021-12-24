package org.hsmak.classification

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object ClassificationOnTitanic extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/titanic"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("ClassificationOnTitanic")
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


  import spark.implicits._

  val passengers = titanicPassengersDF.select(
    $"Pclass",
    $"Survived".cast(DoubleType).as("Survived"),
    $"Gender", // categorical data
    $"Age",
    $"SibSp",
    $"Parch",
    $"Fare")
  passengers.show(5)

  // VectorAssembler does not support the StringType type. So convert Gender to numeric

  val genderIndexer = new StringIndexer()
    .setInputCol("Gender")
    .setOutputCol("GenderCat")

  //Fit the Categorical gender data into the DF
  val passengersWithCategoricalGender = genderIndexer.fit(passengers).transform(passengers)

  passengersWithCategoricalGender.show(5)


  //Remove rows that have null values
  val passengersNoNull = passengersWithCategoricalGender.na.drop()
  println(
    s"""Orig = ${passengersWithCategoricalGender.count()}
       |Final = ${passengersNoNull.count()}
       |Dropped = ${(passengersWithCategoricalGender.count() - passengersNoNull.count())}""".stripMargin)


  // Extract Features using VectorAssembler

  val assembler = new VectorAssembler()
    .setInputCols(Array("Pclass", "GenderCat", "Age", "SibSp", "Parch", "Fare"))
    .setOutputCol("features")

  val passengersWithFeatures = assembler.transform(passengersNoNull)

  passengersWithFeatures.show(5)


  // Split Data using df.randomSplit()

  val Array(trainData, testData) = passengersWithFeatures.randomSplit(Array(0.9, 0.1))
  println(
    s"""Train = ${trainData.count()}
       |Test = ${testData.count()}""".stripMargin)

  // Train a DecisionTree model.

  val dTClassifier = new DecisionTreeClassifier()
    .setLabelCol("Survived")
    .setImpurity("gini") // could be "entropy"
    .setMaxBins(32)
    .setMaxDepth(5)

  val mdlTree = dTClassifier.fit(trainData) // the time-consuming operation

  println(
    s"""Begin...
       |The tree has ${mdlTree.numNodes} nodes.
       |${mdlTree.toDebugString}
       |${mdlTree.toString}
       |${mdlTree.featureImportances}
       |End...""".stripMargin)

  // predict on TestSet and calculate accuracy

  val predictions = mdlTree.transform(testData)
  predictions.show(5)

  // Evaluating the Model

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("Survived")
    .setMetricName("accuracy") // could be f1, "weightedPrecision" or "weightedRecall" TODO - find all possible metrics

  val accuracy = evaluator.evaluate(predictions)
  println("Test Accuracy = %.2f%%".format(accuracy * 100))

}
