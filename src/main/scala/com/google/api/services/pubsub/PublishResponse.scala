package com.google.api.services.pubsub

import scala.collection.JavaConverters._

object PublishResponse {

  type MessageId = String

  def apply(javaResponse: model.PublishResponse): List[MessageId] = {
    val response = Option(javaResponse.getMessageIds)
    if (response.isDefined)
      response.get.asScala.toList
    else List.empty[MessageId]
  }

}
