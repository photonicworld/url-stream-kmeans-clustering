package com.classifier.data

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, JsonNode, ObjectMapper}
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Converter {

  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.registerModule(new JodaModule)
  objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)

  def byteToJsonNode(json: Array[Byte]): JsonNode = {
    objectMapper.readValue[JsonNode](json)
  }

  def convertToEvent(json: Array[Byte]): Event = {
    try {
      objectMapper.readValue[Event](json)
    }
    catch {
      case jMappingE: JsonMappingException => {
        val jsonDecoded = objectMapper.readTree(json).asText()
        //println(s"Warn: problem deserializing json ${jsonDecoded}")
        return objectMapper.readValue[Event](jsonDecoded.getBytes)
      };
    }
  }

  def convertToJoinedEventToUrlAndMessage(json: Array[Byte]): (Event, Event) = {
    val joinedEvent = convertToEvent(json)
    var urlEvent: Event = null;
    var messageEvent: Event = null;
    if (joinedEvent != null) {
      val joinedEventsList = joinedEvent.data.get("events").get.asInstanceOf[List[Map[String, Any]]]

      for (i <- 0 to joinedEventsList.size - 1) {
        val event = joinedEventsList(i)
        val eventType = event.get("type").get;
        if (eventType.equals("UrlFeed")) {
          val eventJson = objectMapper.writeValueAsString(event.get("event").get)
          urlEvent = convertToEvent(eventJson.getBytes)
        }
        else if (eventType.equals("Message")) {
          val eventJson = objectMapper.writeValueAsString(event.get("event").get)
          messageEvent = convertToEvent(eventJson.getBytes)
        }
      }
    }
    return (urlEvent, messageEvent)
  }

}
