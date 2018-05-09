package com.classifier.data

import org.scalatest.FlatSpec

import scala.io.Source

/**
  * Created by satish on 10/16/16.
  */
class TestConverter extends FlatSpec {

  it must "Deserialize to event object from json" in {
    val source = Source.fromURL(getClass.getResource("/attachment.json"));
    val requestJson = source.mkString
    val e = Converter.byteToJsonNode(requestJson.getBytes)
    assert(e != null)
  }

  it must "Deserialize to kafka event object from json" in {
    val source = Source.fromURL(getClass.getResource("/urlfeed.json"))
    val requestJson = source.mkString
    val event = Converter.convertToEvent(requestJson.getBytes)
    assert(event != null)
  }

  it must "Deserialize to kafka joined event object from json" in {
    val source = Source.fromURL(getClass.getResource("/JoinedMessageUrlThreat.json"))
    val requestJson = source.mkString
    val event = Converter.convertToJoinedEventToUrlAndMessage(requestJson.getBytes)
    assert(event._1 != null)
    assert(event._2 != null)
  }
}
