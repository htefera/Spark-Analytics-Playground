package org.snithish

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel, linalg}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Classification {
  val labelCol = "spam"
  val predictionCol = "spam_predicted"

  def evaluation(predictions: DataFrame, model: String): Unit = {
    val eval = predictions.select(labelCol, predictionCol).cache()
    val areaUnderROC = new BinaryClassificationEvaluator().setLabelCol(labelCol).setRawPredictionCol(predictionCol).evaluate(eval)
    val trueFilter = eval.where(col("spam") === col("spam_predicted")).cache()
    val falseFilter = eval.where(col("spam") =!= col("spam_predicted")).cache()

    def confusionMatrixGen(df: DataFrame, label: Double) = {
      df.where(col(labelCol) === label).count()
    }

    val truePositive = confusionMatrixGen(trueFilter, 1.0)
    val trueNegative = confusionMatrixGen(trueFilter, 0.0)

    val falseNegative = confusionMatrixGen(falseFilter, 1.0)
    val falsePositive = confusionMatrixGen(falseFilter, 0.0)
    println("=============================================================================================")
    println(model)
    println(s"RoC: $areaUnderROC")
    println(s"${"%5s".format("a\\p")} | ${"%5s".format("spam")} | ${"%5s".format("ham")}")
    println(s"${"%5s".format("spam")} | ${"%5s".format(truePositive)} | ${"%5s".format(falseNegative)}")
    println(s"${"%5s".format("ham")} | ${"%5s".format(falsePositive)} | ${"%5s".format(trueNegative)}")
    println(s"Accuracy: ${(truePositive.toDouble + trueNegative) / (truePositive + trueNegative + falsePositive + falseNegative)}")
    println(s"Precision: ${(truePositive.toDouble) / (truePositive + falsePositive)}")
    println(s"Recall: ${(truePositive.toDouble) / (truePositive + falseNegative)}")
    println("=============================================================================================")
  }

  def main(args: Array[String]): Unit = {
    val sessionBuilder = SparkSession.builder().master("local[*]")
    val spark = sessionBuilder.appName("Spam Classification").getOrCreate()
    val spamTrain = readFile(spark, "./data/spam_training.txt", labelCol, isSpamData = true)
    val spamTest = readFile(spark, "./data/spam_testing.txt", labelCol, isSpamData = true)
    val nospamTrain = readFile(spark, "./data/nospam_training.txt", labelCol, isSpamData = false)
    val nospamTest = readFile(spark, "./data/nospam_testing.txt", labelCol, isSpamData = false)
    val rawTraining = spamTrain.unionAll(nospamTrain)
    val rawTesting = spamTest.unionAll(nospamTest)
    val normalize = (text: String) => {
      val normalizedEmail = text.replaceAll("([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]+)", "normalizedEmail")
      val normalizedUrls = normalizedEmail.replaceAll("(http[s]?\\S+)|(\\w+\\.[A-Za-z]{2,4}\\S*)", "normalizedURL")
      val normalizedCurrency = normalizedUrls.replaceAll("£|\\$|€|₹", "normalizedCurrency")
      val normalizedNumbers = normalizedCurrency.replaceAll("\\d+(\\.\\d+)?", "normalizedNumbers")
      normalizedNumbers.toLowerCase
    }
    val normalizeUdf = udf(normalize)
    val training = rawTraining.withColumn("normalizedEmail", normalizeUdf(col("email")))
    val testing = rawTesting.withColumn("normalizedEmail", normalizeUdf(col("email")))
    val pipelineModel: PipelineModel = getPipelineModel(training)
    val transformedTraining = getLabeledPoint(pipelineModel, training).cache()
    val transformedTesting = getLabeledPoint(pipelineModel, testing).cache()
    trainEvaluate(spark, new LogisticRegressionWithLBFGS().setNumClasses(2), transformedTraining, transformedTesting)
    trainEvaluate(spark, new SVMWithSGD(), transformedTraining, transformedTesting)
  }

  def trainEvaluate[T <: GeneralizedLinearModel](spark: SparkSession, model: GeneralizedLinearAlgorithm[T], training: RDD[LabeledPoint], testing: RDD[LabeledPoint]): T = {
    val trainedModel = model.run(training)
    val predictionsWithLabel = testing.map { case LabeledPoint(label, features) =>
      val prediction = trainedModel.predict(features)
      (label, prediction)
    }
    import spark.implicits._
    evaluation(predictionsWithLabel.toDF(labelCol, predictionCol), model.toString)
    trainedModel
  }

  def getLabeledPoint(pipelineModel: PipelineModel, dataFrame: DataFrame): RDD[LabeledPoint] = {
    pipelineModel.transform(dataFrame).select("features", "spam").rdd.map(x => LabeledPoint(x.getAs[Double]("spam"), Vectors.fromML(x.getAs[linalg.Vector]("features"))))
  }


  private def getPipelineModel(training: Dataset[Row]): PipelineModel = {
    val tokenizer = new RegexTokenizer().setInputCol("normalizedEmail").setOutputCol("emailTk").setPattern("\\s+|[,.()\"]")
    val stopWords = new StopWordsRemover().setStopWords(StopWordsRemover.loadDefaultStopWords("english")).setInputCol("emailTk").setOutputCol("emailToken")
    val tf = new HashingTF().setInputCol("emailToken").setOutputCol("emailTF")
    val idf = new IDF().setInputCol("emailTF").setOutputCol("features")
    val steps = Array(tokenizer, stopWords, tf, idf)
    val pipeline = new Pipeline().setStages(steps)
    val pipelineModel = pipeline.fit(training)
    pipelineModel
  }

  private def readFile(spark: SparkSession, filePath: String, labelCol: String, isSpamData: Boolean): DataFrame = {
    val label = if (isSpamData) 1.0 else 0.0
    spark.read.text(filePath).withColumn(labelCol, lit(label)).withColumnRenamed("value", "email")
  }
}