package com.classifier

import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class TestProcessUrlFeeds extends FlatSpec with BeforeAndAfter {
  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private var sc: SparkContext = _


  before {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test-spark")
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  it should "process url feeds" in {
    //ProcessUrlFeeds.processUrlFeeds(sc)
  }
}
