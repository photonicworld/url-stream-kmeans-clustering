package com.classifier.data

import org.joda.time.DateTime

case class Event(uuid: String, host: String, timestamp: DateTime, data: Map[String, AnyVal], `type`: String)

trait ThreatEvent {
  def id: String

  def source: String

  def forensics: Map[String, Any]
}

case class UrlEvent(id: String, url: String, source: String, classification: Option[String], forensics: Map[String, Any]) extends ThreatEvent

case class AttachmentEvent(id: String, sha256: String, source: String, forensics: Map[String, Any]) extends ThreatEvent

