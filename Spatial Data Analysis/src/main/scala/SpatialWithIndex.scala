package org.haftamu

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import java.io.{BufferedWriter, File, PrintWriter}

case class IndexRunConfig(
  tweetIndex: Option[IndexType],
  neighborhoodIndex: Option[IndexType],
  outputPath: String,
  label: String
)

object SpatialWithIndex {
  private val tweetPath = "./data/nyc-tweets.txt"
  private val neighborhoodPath = "./data/nyc-neighborhoods.wkt"

  def main(args: Array[String]): Unit = {
    val configs = Array(
      IndexRunConfig(None, None, "d_one.csv", "No Index"),
      IndexRunConfig(
        None,
        Some(IndexType.RTREE),
        "d_neighborhood_rtree.csv",
        "R Tree Index on Neighborhood"
      ),
      IndexRunConfig(Some(IndexType.RTREE), None, "d_tweet_rtree.csv", "R Tree Index on Tweets"),
      IndexRunConfig(
        Some(IndexType.RTREE),
        Some(IndexType.RTREE),
        "d_rtree.csv",
        "R Tree Index on Both"
      ),
      IndexRunConfig(
        None,
        Some(IndexType.QUADTREE),
        "d_neighborhood_quadtree.csv",
        "Quad Tree Index on Neighborhood"
      ),
      IndexRunConfig(
        Some(IndexType.QUADTREE),
        None,
        "d_tweet_quadtree.csv",
        "Quad Tree Index on Tweets"
      ),
      IndexRunConfig(
        Some(IndexType.QUADTREE),
        Some(IndexType.QUADTREE),
        "d_quadtree.csv",
        "Quad Tree Index on Both"
      ),
    )

    val metrics = configs.map(x => (x.label, execute(x)))

    val bufferedPrintWriter = new BufferedWriter(
      new PrintWriter(new File("./data/metrics.csv"))
    )
    metrics.foreach(x => {
      bufferedPrintWriter.write(s"${x._1} took ${(x._2 / 1000000.00).toString}ms")
      bufferedPrintWriter.newLine()
    })
    bufferedPrintWriter.close()
  }

  private def execute(runConfig: IndexRunConfig) = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SpatialQuery")
      .config("spark.executor.memory", "10g")
      .config("spark.driver.memory", "30g")
      .config("spark.shuffle.spill", "false")
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext
    val startTime = System.nanoTime()
    val (tweetSpatialRdd, neighbourhoodSpatialRdd) =
      Utils.inputs(sc, tweetPath, neighborhoodPath)

    runConfig.tweetIndex.foreach(x => tweetSpatialRdd.buildIndex(x, true))
    runConfig.neighborhoodIndex.foreach(x => neighbourhoodSpatialRdd.buildIndex(x, true))

    Utils.tweetCountPersistence(
      spark,
      tweetSpatialRdd,
      neighbourhoodSpatialRdd,
      s"./data/${runConfig.outputPath}",
      runConfig.tweetIndex.isDefined || runConfig.neighborhoodIndex.isDefined
    )
    System.nanoTime() - startTime
  }
}
