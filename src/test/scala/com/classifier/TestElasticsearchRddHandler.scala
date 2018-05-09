package com.classifier

import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.io.Source

class TestElasticsearchRddHandler extends FlatSpec with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private var sc: SparkContext = _
  private var requestJson: String = _


  before {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test-spark")
    sc = new SparkContext(conf)
    val source = Source.fromURL(getClass.getResource("/attachment.json"));
    requestJson = source.mkString
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  /**
    * Ignore this test case as this is an integration test
    */
  ignore should "insert to elasticsearch" in {
    //val eventSeq = Seq(Converter.jsonToEvent(UUID.randomUUID().toString, requestJson))
    //val eventRDD = sc.parallelize(eventSeq)
    //ElasticsearchRddHandler.insertEventToES(eventRDD)
  }
}
