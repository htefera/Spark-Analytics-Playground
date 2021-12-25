package org.haftamu

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{avg, coalesce, col, lit}
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class BaselinePredictor(override val uid: String) extends Estimator[BaselineModel] {
  var userCol: String = "userId"
  var itemCol: String = "itemId"
  var ratingCol: String = "rating"
  var predictionCol: String = "prediction"

  def this() = this(Identifiable.randomUID("baseline"))

  def setUserCol(value: String): this.type = {
    userCol = value
    this
  }

  def setItemCol(value: String): this.type = {
    itemCol = value
    this
  }

  def setRatingCol(value: String): this.type = {
    ratingCol = value
    this
  }

  def setPredictionCol(value: String): this.type = {
    predictionCol = value
    this
  }

  override def fit(dataset: Dataset[_]): BaselineModel = {
    transformSchema(dataset.schema)
    dataset.selectExpr(userCol, itemCol, s"cast(${ratingCol} as float) as ${ratingCol}")
    val mu = dataset.select(avg(ratingCol)).collect().map(x => x.getAs[Double](0)).head
    val userBias = dataset.groupBy(userCol).agg((avg(ratingCol) - lit(mu)).as("userBias"))
    val itemBias = dataset.groupBy(itemCol).agg((avg(ratingCol) - lit(mu)).as("itemBias"))
    new BaselineModel(uid, mu, userBias, itemBias)
      .setItemCol(itemCol)
      .setUserCol(userCol)
      .setPredictionCol(predictionCol)
  }

  override def copy(extra: ParamMap): Estimator[BaselineModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val col = StructField("predictions", FloatType, nullable = false)
    StructType(schema.fields :+ col)
  }
}

class BaselineModel(override val uid: String, mu: Double, userBias: DataFrame, itemBias: DataFrame)
    extends Model[BaselineModel] {
  var userCol: String = "userId"
  var itemCol: String = "itemId"
  var ratingCol: String = "rating"
  var predictionCol: String = "prediction"

  def setUserCol(value: String): this.type = {
    userCol = value
    this
  }

  def setItemCol(value: String): this.type = {
    itemCol = value
    this
  }

  def setPredictionCol(value: String): this.type = {
    predictionCol = value
    this
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val joinedUserBias = dataset
      .join(userBias, dataset(userCol).equalTo(userBias(userCol)), "leftouter")
      .select(
        dataset.columns.map(x => dataset(x)) :+ coalesce(userBias("userBias"), lit(0.0))
          .as("userBias"): _*,
      )
    val completeBias = joinedUserBias
      .join(itemBias, joinedUserBias(itemCol).equalTo(itemBias(itemCol)), "leftouter")
      .select(
        joinedUserBias.columns
          .map(x => joinedUserBias(x)) :+ coalesce(itemBias("itemBias"), lit(0.0))
          .as("itemBias"): _*,
      )
    val res = completeBias.withColumn(predictionCol, col("itemBias") + col("userBias") + lit(mu))
    val cols = dataset.columns :+ predictionCol
    res.select(cols.map(x => res(x)): _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    val col = StructField("predictions", FloatType, nullable = false)
    StructType(schema.fields :+ col)
  }

  override def copy(extra: ParamMap): BaselineModel = defaultCopy(extra)
}
