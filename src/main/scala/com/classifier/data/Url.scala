package com.classifier.data

case class Url(id: Long, url: String, domainHash: Long, hostnameHash: Long, pathHash: Long, classification: String, classificationHash: Long)

case class UrlMessage(id: Long, url: String, domainHash: Long, hostnameHash: Long, pathHash: Long, classification: String, classificationHash: Long, messageId: String)

case class StoredUrlThreat(id: Long, url: String, classification: String, cluster: Int, messageId: String, createdAt: String)
