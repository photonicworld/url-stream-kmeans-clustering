package com.classifier

import com.classifier.data.StoredUrlThreat
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.Properties

/**
  * Created by satish on 10/15/16.
  */
object ElasticsearchRddHandler {

  val elasticSearchHost = Properties.envOrElse("ELASTICSEARCH_HOST", "localhost")
  val elasticSearchPort = Properties.envOrElse("ELASTICSEARCH_PORT", "9200")

  val esConfig = Map("es.index.auto.create" -> "true",
    "es.nodes" -> elasticSearchHost,
    "es.port" -> elasticSearchPort,
    "es.http.timeout" -> "4s")

  val ES_DATE_FORMAT = "YYYY-MM-dd'T'HH:mm:ss"
  val ES_DATE_INDEX_FORMAT = "YYYY-MM-dd"

  val log = Logger.getLogger(ElasticsearchRddHandler.getClass)

  def insertEventToES(eventRDD: RDD[StoredUrlThreat]): Unit = {
    println(s"Inserting ${eventRDD.count()} events to elasticsearch...")
    EsSpark.saveToEs(eventRDD,
      esConfig +
        ("es.resource.write" -> "url_message/url_message",
          "es.mapping.id" -> "id")
    )
  }
}
