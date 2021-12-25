package org.snithish

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.formatMapper.WktReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object Utils {

  private def neighbourhood(sc: SparkContext, filePath: String): SpatialRDD[Geometry] = {
    val neighbourhoodRdd = sc
      .textFile(filePath)
      .zipWithIndex()
      .map(x => x._1.trim + "\t" + x._2.toString)
      .toJavaRDD()
    WktReader.readToGeometryRDD(
      neighbourhoodRdd,
      0,
      true,
      false
    )
  }

  private def tweets(sc: SparkContext, filePath: String): SpatialRDD[Geometry] = {
    val tweetRawRdd = sc
      .textFile(filePath)
      .map(line => (line, 1))
      .reduceByKey((i, i1) => i + i1)
      .map(x => {
        val coords = x._1.split(",")
        s"POINT (${coords(0)} ${coords(1)})\t${x._2.toString}"
      })
      .toJavaRDD()
    WktReader.readToGeometryRDD(tweetRawRdd, 0, true, false)
  }

  def inputs(sc: SparkContext, tweetPath: String, neighborhoodPath: String) = {
    val tweetSpatialRdd = tweets(sc, tweetPath)
    tweetSpatialRdd.analyze()
    tweetSpatialRdd.spatialPartitioning(GridType.RTREE)

    val neighbourhoodSpatialRdd = neighbourhood(sc, neighborhoodPath)
    neighbourhoodSpatialRdd.spatialPartitioning(tweetSpatialRdd.getPartitioner)
    (tweetSpatialRdd, neighbourhoodSpatialRdd)
  }

  def tweetCountPersistence(
    spark: SparkSession,
    tweetSpatialRdd: SpatialRDD[Geometry],
    neighbourhoodSpatialRdd: SpatialRDD[Geometry],
    outputPath: String,
    useIndex: Boolean
  ) = {
    import spark.implicits._
    val intersectingPoints = JoinQuery
      .SpatialJoinQueryFlat(
        tweetSpatialRdd,
        neighbourhoodSpatialRdd,
        new JoinQuery.JoinParams(useIndex, false, false)
      )
      .rdd
      .map(
        x => (x._1.getUserData.asInstanceOf[String], x._2.getUserData.asInstanceOf[String].toLong)
      )
      .cache()
    val counts = intersectingPoints.reduceByKey((x, y) => x + y)
    counts
      .coalesce(1)
      .map(x => (x._1, x._2))
      .toDF("neighbourHoodId", "totalTweets")
      .write
      .mode("overwrite")
      .csv(outputPath)
  }
}
