package org.haftamu

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object Recommender {

  private val RunExperiment: Boolean = true

  def main(args: Array[String]): Unit = {
    val sessionBuilder = SparkSession.builder().master("local[*]")
    val spark = sessionBuilder.appName("Clustering").getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp")
    val movies = csv_reader(spark, "./data/movies.csv")
    val ratings = csv_reader(spark, "./data/ratings.csv")
    if (RunExperiment) {
      val movieRating = ratings
        .join(movies, Seq("movieId"))
        .select("movieId", "title", "rating", "genres")
        .cache()
      experimentOne(movieRating)
      experimentTwo(movieRating)
      experimentThree(movies, ratings)
    }
    val ratingsCached = ratings.drop("timestamp").cache()
    val Array(training, testing) = ratingsCached.randomSplit(Array(0.7, 0.3)).map(x => x.cache())
    val baselineModel = new BaselinePredictor()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("predictedRating")
      .fit(training)
    val predictions = baselineModel.transform(testing)
    val baselineEval = evaluation(predictions)
    val (colabModel, colabPredictions) = collaborativeFiltering(training, testing)
    val colabEval = evaluation(colabPredictions)
    import spark.implicits._
    val recommendations = colabModel
      .recommendForAllUsers(1)
      .rdd
      .map(
        x =>
          (
            x.getAs[Int]("userId"),
            x.getAs[mutable.WrappedArray[Row]]("recommendations").head.getAs[Int](0)
        )
      )
      .toDF("userId", "recommendedMovieId")
    recommendations.write
      .mode("overwrite")
      .csv("./data/recommendations.csv")
    spark.stop()
    println(s"Root-mean-square error baseline = $baselineEval")
    println(s"Root-mean-square error collaborative filtering = $colabEval")

  }

  private def evaluation(predictions: DataFrame): Double = {
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("predictedRating")
    evaluator.evaluate(predictions)
  }

  def collaborativeFiltering(training: DataFrame, testing: DataFrame): (ALSModel, DataFrame) = {
    val als = new ALS()
      .setMaxIter(50)
      .setRegParam(0.09)
      .setRank(25)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setCheckpointInterval(2)
      .setNonnegative(true)
    val model = als.fit(training).setPredictionCol("predictedRating")
    model.setColdStartStrategy("drop")
    (model, model.transform(testing))
  }

  private def experimentThree(movies: DataFrame, ratings: DataFrame): Unit = {
    val firstHundredMovies = movies.select("movieId").limit(100).cache()
    val cogroupedMovies =
      firstHundredMovies
        .as("one")
        .crossJoin(firstHundredMovies.as("other"))
        .select(col("one.movieId").as("firstMovieId"), col("other.movieId").as("secondMovieId"))
        .where("firstMovieId <> secondMovieId")
    val userRating = ratings.select("movieId", "userId").distinct()
    val usersWhoRatedBothMovies = cogroupedMovies
      .join(userRating.as("firstMovieRating"))
      .join(userRating.as("secondMovieRating"))
      .where(
        "firstMovieId = firstMovieRating.movieId and secondMovieId = secondMovieRating.movieId"
      )
      .where("firstMovieRating.userId = secondMovieRating.userId")
      .selectExpr("firstMovieId", "secondMovieId", "firstMovieRating.userId")

    val support = usersWhoRatedBothMovies
      .groupBy("firstMovieId", "secondMovieId")
      .agg(count("userId").as("support"))
    support.write
      .mode("overwrite")
      .csv("./data/c3.csv")
  }

  private def experimentOne(movieRating: DataFrame): Unit = {
    val topTenRatedMovie = movieRating
      .groupBy("movieId")
      .agg(first("title").as("title"), count("rating").as("ratingsCount"))
      .sort(desc("ratingsCount"))
      .select("title")
      .limit(10)
    topTenRatedMovie
      .coalesce(1)
      .write
      .mode("overwrite")
      .csv("./data/c1.csv")
  }

  private def experimentTwo(movieRating: DataFrame): Unit = {
    val topTenAvgRating = movieRating
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .drop("genres")
      .groupBy("genre", "movieId")
      .agg(first("title").as("title"), avg("rating").as("averageRating"))
      .sort(desc("averageRating"))
      .select("title", "genre")
      .limit(10)
    topTenAvgRating.write
      .mode("overwrite")
      .csv("./data/c2.csv")
  }

  private def csv_reader(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(filePath)
      .cache()
  }
}
