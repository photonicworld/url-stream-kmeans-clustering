package com.classifier

import com.classifier.data.Converter
import org.apache.log4j.LogManager
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Properties


object ProcessUrlFeeds {

  case class Url(url: String, classification: String)

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger

    val batchInterval = Seconds(10)
    val appName = Properties.envOrElse("APP_NAME", "threat_co-relation")
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val session = SparkSession.builder().appName(appName).config(conf).getOrCreate()
    val sc = session.sparkContext
    val sqlContext = session.sqlContext
    import sqlContext.implicits._
    val urlRdd = processUrlFeeds(sc, sqlContext)
    val urlDF = urlRdd.toDF()
    urlDF.cache()
    urlDF.show()
    val kMeansPredictionModel = setupPipeLine(urlRdd.toDF)
    val predictionResult = kMeansPredictionModel.transform(urlDF)
    predictionResult.select("url", "classification", "urlIndex", "classificationIndex", "prediction").show(100, true)
    predictionResult.select("prediction").groupBy("prediction").count().show()

  }

  def setupPipeLine(urlDF: DataFrame): PipelineModel = {
    val urlIndexer = new StringIndexer().setInputCol("url").setOutputCol("urlIndex")
    val urlEncoder = new OneHotEncoder().setInputCol("urlIndex").setOutputCol("urlVec")

    val classificationIndexer = new StringIndexer().setInputCol("classification").setOutputCol("classificationIndex")
    val classificationEncoder = new OneHotEncoder().setInputCol("classificationIndex").setOutputCol("classificationVec")

    val assembler = new VectorAssembler().setInputCols(Array("classificationVec", "urlVec")).setOutputCol("features")
    val kmeans = new KMeans().setK(20).setFeaturesCol("features").setPredictionCol("prediction")
    val pipeline = new Pipeline().setStages(Array(urlIndexer, urlEncoder, classificationIndexer, classificationEncoder, assembler, kmeans))
    val kMeansPredictionModel = pipeline.fit(urlDF)
    kMeansPredictionModel
  }

  def processUrlFeeds(sc: SparkContext, sqlContext: SQLContext): RDD[Url] = {
    val urlFeeds = sc.textFile(getClass.getResource("/urlfeed_training.json").getFile)
    val urls = urlFeeds
      .map(feed => Converter.convertToEvent(feed.getBytes))
      .map(event => Url(event.data.get("url").getOrElse("").toString, event.data.get("classification").getOrElse(List[String]()).asInstanceOf[List[String]].head.toString))
    urls
  }
}
