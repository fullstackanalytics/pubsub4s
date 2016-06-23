package com.google.api.services.pubsub

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._

// create a Serializable type that can be passed to Akka actors and flows

final case class ReceivedMessage(ackId: String, time: String, data: Array[Byte], attributes: MMap[String,String])

object PullResponse {

  type PullResponse = List[ReceivedMessage]

  def apply(javaResponse: model.PullResponse): PullResponse =
    javaResponse.getReceivedMessages().asScala.toList
      .map(r => {
        val msg = r.getMessage()
        ReceivedMessage(
          r.getAckId(),
          msg.getPublishTime(),
          msg.decodeData(),
          msg.getAttributes().asScala
        )
      })

}

