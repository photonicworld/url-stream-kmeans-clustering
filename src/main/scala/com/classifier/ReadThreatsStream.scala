package com.classifier

import com.classifier.data.Converter._
import kafka.serializer.DefaultDecoder
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Properties

object ReadThreatsStream {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger

    val batchInterval = Seconds(10)
    val appName = Properties.envOrElse("APP_NAME", "threat_co-relation")
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")

    val brokers = "kafka.lab.net:9092"
    val maxBytesToFetch = "5242880"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> maxBytesToFetch)
    val topicSet = Set("UrlFeed", "AttachmentFeed", "Message")

    val streamingContext = new StreamingContext(conf, batchInterval)
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](streamingContext, kafkaParams, topicSet)

    stream.foreachRDD((rdd, time) => {
      log.warn("-------------------------------------------")
      println(s"Time: $time - Count: ${rdd.count()}")
      println("-------------------------------------------")
      rdd.foreach(eventFromKafka => {
        val jsonNode = byteToJsonNode(byteToJsonNode(eventFromKafka._2).get("payload").binaryValue())
        println(jsonNode)
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
    println("Stopping streaming context")
    streamingContext.stop()
  }
}
