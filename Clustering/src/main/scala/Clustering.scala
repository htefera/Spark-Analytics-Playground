package org.snithish

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedWriter, File, PrintWriter}

object Clustering {

  def main(args: Array[String]): Unit = {
    val sessionBuilder = SparkSession.builder().master("local[*]")
    val spark = sessionBuilder.appName("Clustering").getOrCreate()
    val rawVectors = spark.sparkContext
      .textFile("./data/mnist_test.csv")
      .map(line => {
        val grayScale = line.split(",").map(x => x.toDouble)
        Row(Vectors.dense(grayScale))
      })
    val schema = StructType(List(StructField("input", VectorType, nullable = true)))
    val data = spark.createDataFrame(rawVectors, schema)
    val scaler =
      new StandardScaler()
        .setInputCol("input")
        .setOutputCol("features")
        .setWithStd(true)
        .setWithMean(true)
        .fit(data)
    val featuresDf = scaler.transform(data).cache()
    kmeans(scaler, featuresDf, 10)
    spark.stop()
  }

  private def kmeans(
    scaler: StandardScalerModel,
    featuresDf: DataFrame,
    cluster_size: Int
  ): Unit = {
    val kmeans =
      new KMeans()
        .setK(cluster_size)
        .setMaxIter(100)
        .setFeaturesCol("features")
        .setPredictionCol("clusters")
    val model = kmeans.fit(featuresDf)
    val bufferedPrintWriter = new BufferedWriter(
      new PrintWriter(new File("./data/kmeans_centroids.csv"))
    )
    val scaleParam = scaler.std.toArray.zip(scaler.mean.toArray)
    model.clusterCenters.foreach(x => {
      bufferedPrintWriter.write(
        x.toArray.zip(scaleParam).map(a => a._1 * a._2._1 + a._2._2).mkString(",")
      )
      bufferedPrintWriter.newLine()
    })
    bufferedPrintWriter.close()
  }
}
