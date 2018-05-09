package com.classifier

import com.classifier.data.Converter._
import com.classifier.data.{Converter, Event, StoredUrlThreat, UrlMessage}
import com.classifier.normalizer.Normalizer
import com.classifier.utils.DateUtils
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime

import scala.util.Properties
import scala.util.hashing.MurmurHash3

object ClusterJoinedUrlMessageStream {
  val normalizer = new Normalizer

  def main(args: Array[String]): Unit = {
    val batchInterval = Seconds(30)
    val appName = Properties.envOrElse("APP_NAME", "threat_co-relation")
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]").set("spark.executor.memory", "2g").set("spark.driver.memory", "2g")
    val session = SparkSession.builder().appName(appName).config(conf).getOrCreate()
    val sqlContext = session.sqlContext
    import sqlContext.implicits._

    val brokers = "kafka.lab.net:9092"
    val maxBytesToFetch = "5242880"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> maxBytesToFetch)
    val topicSet = Set("JoinedMessageUrlThreat")

    val streamingContext = new StreamingContext(session.sparkContext, batchInterval)
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](streamingContext, kafkaParams, topicSet)

    transformInputDStream(stream).foreachRDD(url => {
      val urlDF = url.toDF()
      if (urlDF.count() > 0) {
        val kmeansModel = setupPipeLine(urlDF)
        val prediction = kmeansModel.transform(urlDF)
        prediction.cache()
        //prediction.show()
        val toInsert = prediction.map(row => StoredUrlThreat(row.getAs[Long]("id"),
          row.getAs[String]("url"),
          row.getAs[String]("classification"),
          row.getAs[Int]("prediction"),
          row.getAs[String]("messageId"),
          DateUtils.toDateString(DateTime.now()))).rdd

        ElasticsearchRddHandler.insertEventToES(toInsert)
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

  def transformInputDStream(stream: InputDStream[(Array[Byte], Array[Byte])]): DStream[UrlMessage] = {
    stream.map(keyVal => keyVal._2).map(feed => {
      val feedBytes = byteToJsonNode(feed).get("payload").binaryValue()
      Converter.convertToJoinedEventToUrlAndMessage(feedBytes)
    }).map(event => convertEventToUrl(event))
  }

  def convertEventToUrl(event: (Event, Event)): UrlMessage = {
    var messageEvent: Event = null
    var urlEvent: Event = null
    if (event._1.`type`.equals("UrlFeed")) {
      urlEvent = event._1
      messageEvent = event._2
    }
    else {
      messageEvent = event._1
      urlEvent = event._2
    }


    val url = urlEvent.data.get("url").getOrElse("").toString
    val classification = urlEvent.data.get("classification").getOrElse(List[String]()).asInstanceOf[List[String]].head.toString
    val normalizedUrl = normalizer.normalize(url)
    val domainHash = MurmurHash3.stringHash(normalizedUrl.getDomainname)
    val pathHash = MurmurHash3.stringHash(normalizedUrl.getPath)
    val hostnameHash = MurmurHash3.stringHash(normalizedUrl.getHostname)
    val classificationHash = MurmurHash3.stringHash(classification)
    val messageId = messageEvent.data.get("originalMessageId").getOrElse("").toString
    val id = MurmurHash3.stringHash(url + messageId)

    UrlMessage(id, url, domainHash, hostnameHash, pathHash, classification, classificationHash, messageId)
  }

}
