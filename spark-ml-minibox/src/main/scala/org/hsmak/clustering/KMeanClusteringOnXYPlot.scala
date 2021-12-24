package org.hsmak.clustering

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession}

object KMeanClusteringOnXYPlot extends App {

  Configurator.setLevel("org.apache.spark", Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"


   /* ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("KMeanClusteringOnXYPlot")
    .getOrCreate()

  import spark.implicits._


   /* ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  //Load the Data
  val xyPlotDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/cluster-points-v2.csv")


  println("carMileageDF has " + xyPlotDF.count() + " rows")
  xyPlotDF.show(5)
  xyPlotDF.printSchema()

  //Extract features
  val assembler = new VectorAssembler()
    .setInputCols(Array("X", "Y"))
    .setOutputCol("features")

  val xyPlotWithFeatures = assembler.transform(xyPlotDF)
  xyPlotWithFeatures.show

  // Train the K-Means model
  var algKMeans = new KMeans()
    .setK(2)
  // set it for 2 clusters. You may experiment with 3 or 4 clusters.
  var mdlKMeans = algKMeans.fit(xyPlotWithFeatures)

  // Print the summary.
  println("Cluster Centers (K=2) : " + mdlKMeans.clusterCenters.mkString("<", ",", ">"))
  println("Cluster Sizes (K=2) : " + mdlKMeans.summary.clusterSizes.mkString("<", ",", ">"))


  // Deprecated
  // Evaluate clustering by computing Within Set Sum of Squared Errors.
  //  var WSSSE = mdlKMeans.computeCost(xyPlotWithFeatures)
  //  println(s"Within Set Sum of Squared Errors (K=2) = %.3f".format(WSSSE))
  ////////////////////////////////

  //Predict using the Model
  val predict2K = mdlKMeans.transform(xyPlotWithFeatures)
  predict2K.show

  predict2K.select($"X", $"Y", $"prediction")
    .write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv(s"${base_data_dir}/k-means-out/cluster-2K.csv")

  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predict2K)
  println(s"Silhouette with squared euclidean distance = $silhouette")

  // Shows the result.
  println("Cluster Centers/Centroids: ")
  mdlKMeans.clusterCenters.foreach(println)

}
