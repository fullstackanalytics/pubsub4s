package com.google.api.services.pubsub

import scala.collection.JavaConverters._

// create a Serializable type that can be passed to Akka actors and flows

final case class ReceivedMessage(ackId: String, message: PubsubMessage)

object PullResponse {

  def apply(javaResponse: model.PullResponse): List[ReceivedMessage] =
    javaResponse.getReceivedMessages().asScala.toList
      .map(r => ReceivedMessage(r.getAckId(), PubsubMessage(r.getMessage())))

}

