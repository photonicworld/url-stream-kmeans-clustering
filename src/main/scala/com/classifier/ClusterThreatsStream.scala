package com.classifier

import com.classifier.data.Converter._
import com.classifier.data.{Converter, Event, Url}
import com.classifier.normalizer.Normalizer
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Properties
import scala.util.hashing.MurmurHash3

object ClusterThreatsStream {
  val normalizer = new Normalizer

  def main(args: Array[String]): Unit = {
    val batchInterval = Seconds(10)
    val appName = Properties.envOrElse("APP_NAME", "threat_co-relation")
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val session = SparkSession.builder().appName(appName).config(conf).getOrCreate()
    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val brokers = "kafka.lab.net:9092"
    val maxBytesToFetch = "5242880"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> maxBytesToFetch)
    val topicSet = Set("UrlFeed")

    val streamingContext = new StreamingContext(session.sparkContext, batchInterval)
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](streamingContext, kafkaParams, topicSet)

    transformInputDStream(stream).foreachRDD(url => {
      val urlDF = url.toDF()
      if (urlDF.count() > 0) {
        val kmeansModel = setupPipeLine(urlDF)
        val prediction = kmeansModel.transform(urlDF)
        prediction.select("id", "url", "classification", "prediction").show()
      }
      else {
        println("no urls found")
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
    println("Stopping streaming context")
    streamingContext.stop()
  }

  def setupPipeLine(urlDF: DataFrame): PipelineModel = {
    val assembler = new VectorAssembler().setInputCols(Array("classificationHash", "domainHash", "hostnameHash", "pathHash")).setOutputCol("features")
    val kmeans = new KMeans()
      .setK(20)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(25)
    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val kMeansPredictionModel = pipeline.fit(urlDF)
    kMeansPredictionModel
  }

  def transformInputDStream(stream: InputDStream[(Array[Byte], Array[Byte])]): DStream[Url] = {
    stream.map(keyVal => keyVal._2).map(feed => {
      val feedBytes = byteToJsonNode(feed).get("payload").binaryValue()
      Converter.convertToEvent(feedBytes)
    }).map(event => convertEventToUrl(event))
  }

  def convertEventToUrl(event: Event): Url = {
    val url = event.data.get("url").getOrElse("").toString
    val classification = event.data.get("classification").getOrElse(List[String]()).asInstanceOf[List[String]].head.toString

    val normalizedUrl = normalizer.normalize(url)
    val domainHash = MurmurHash3.stringHash(normalizedUrl.getDomainname)
    val pathHash = MurmurHash3.stringHash(normalizedUrl.getPath)
    val hostnameHash = MurmurHash3.stringHash(normalizedUrl.getHostname)
    val classificationHash = MurmurHash3.stringHash(classification)
    val id = MurmurHash3.stringHash(url)

    Url(id, url, domainHash, hostnameHash, pathHash, classification, classificationHash)
  }

}
