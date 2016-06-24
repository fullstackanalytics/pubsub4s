package com.google.api.services.pubsub

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._

// create a Serializable type that can be passed to Akka actors and flows

final case class ReceivedMessage(ackId: String, time: Option[String], data: Array[Byte], attributes: Option[MMap[String,String]])

object PullResponse {

  def apply(javaResponse: model.PullResponse): List[ReceivedMessage] =
    javaResponse.getReceivedMessages().asScala.toList
      .map(r => {
        val msg = r.getMessage()
        ReceivedMessage(
          r.getAckId(),
          msg.getPublishTime(),
          msg.decodeData(),
          msg.getAttributes().map(_.asScala)
        )
      })

  implicit def null2Option[T](o: T): Option[T] = o match {
    case null => None
    case _ => Some(o)
  }
}

