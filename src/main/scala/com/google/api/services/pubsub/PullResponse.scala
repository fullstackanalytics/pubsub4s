package com.google.api.services.pubsub

import scala.collection.JavaConverters._

// wrapper type that can be passed to Akka actors and flows and guards against nulls.

final case class ReceivedMessage(ackId: String, message: PubsubMessage)

object PullResponse {

  def apply(javaResponse: model.PullResponse): List[ReceivedMessage] = {
    val response = Option(javaResponse.getReceivedMessages)
    if (response.isDefined)
      for (
        message <- response.get.asScala.toList;
        body <- Option(message.getMessage);
        ackId = message.getAckId
      ) yield ReceivedMessage(ackId, PubsubMessage(body))
    else List.empty[ReceivedMessage]
  }

}

