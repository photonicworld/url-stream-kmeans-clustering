package com.classifier

import com.classifier.data.Converter
import com.classifier.normalizer.Normalizer
import org.apache.log4j.LogManager
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Properties
import scala.util.hashing.MurmurHash3


object ProcessUrlFeedsWithHash {

  case class Url(url: String, domainHash: Long, hostnameHash: Long, pathHash: Long, classification: String, classificationHash: Long)

  val normalizer = new Normalizer

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
    predictionResult.select("url", "classification", "domainHash", "hostnameHash", "pathHash", "classificationHash", "prediction").show(100, false)
    predictionResult.select("prediction").groupBy("prediction").count().show(true)
  }

  def setupPipeLine(urlDF: DataFrame): PipelineModel = {
    val assembler = new VectorAssembler().setInputCols(Array("classificationHash", "domainHash", "hostnameHash", "pathHash")).setOutputCol("features")
    val kmeans = new KMeans().setK(20).setFeaturesCol("features").setPredictionCol("prediction")
    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val kMeansPredictionModel = pipeline.fit(urlDF)
    kMeansPredictionModel
  }

  def processUrlFeeds(sc: SparkContext, sqlContext: SQLContext): RDD[Url] = {
    val urlFeeds = sc.textFile(getClass.getResource("/urlfeed_training.json").getFile)
    val urls = urlFeeds
      .map(feed => Converter.convertToEvent(feed.getBytes))
      .map(event => {
        val url = event.data.get("url").getOrElse("").toString
        val classification = event.data.get("classification").getOrElse(List[String]()).asInstanceOf[List[String]].head.toString

        val normalizedUrl = normalizer.normalize(url)
        val domainHash = MurmurHash3.stringHash(normalizedUrl.getDomainname)
        val pathHash = MurmurHash3.stringHash(normalizedUrl.getPath)
        val hostnameHash = MurmurHash3.stringHash(normalizedUrl.getHostname)

        val classificationHash = MurmurHash3.stringHash(classification)
        Url(url, domainHash, hostnameHash, pathHash, classification, classificationHash)
      })
    urls
  }
}
