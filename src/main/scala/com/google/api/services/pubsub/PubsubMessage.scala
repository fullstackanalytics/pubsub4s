package com.google.api.services.pubsub

import scala.collection.JavaConverters._
import model.{PubsubMessage => javaPubsubMessage}

import scala.collection.mutable.{Map => MMap}
import scala.util.{Failure, Success, Try}
import scala.language.implicitConversions._


/**
  * A message data and its attributes.
  * The message payload must not be empty; it must contain either a non-empty data field, or at least one attribute.
  *
  * messageId and publishTime are set by pubsub, unless converting from received message.
  */
final case class PubsubMessage(messageId: Option[String],
                               data: Option[Array[Byte]],
                               attributes: Option[MMap[String,String]],
                               publishTime: Option[String]
)

case class InvalidPubsubMesage(err: String) extends Exception

object PubsubMessage {

  def apply(message: javaPubsubMessage): PubsubMessage =
    PubsubMessage(
      Option(message.getMessageId()),
      Option(message.decodeData()),
      Option(message.getAttributes()).map(_.asScala),
      Option(message.getPublishTime())
    )

  def apply(data: Array[Byte]): PubsubMessage = PubsubMessage(None, Some(data), None, None)

  def apply(data: Array[Byte], attributes: MMap[String, String]): PubsubMessage =
    PubsubMessage(None, Some(data), Some(attributes), None)

  def isValid(m: PubsubMessage) =
    m.data.isDefined || (m.attributes.isDefined && m.attributes.nonEmpty)

  def asJava(m: PubsubMessage): Try[javaPubsubMessage] =
    if (isValid(m)) {
      val msg = new javaPubsubMessage()
      m.messageId match { case Some(id) => msg.setMessageId(id); case _ => () }
      m.data match { case Some(d) => msg.encodeData(d); case _ => () }
      m.attributes match { case Some(a) => msg.setAttributes(a.asJava); case _ => () }
      m.publishTime match { case Some(t) => msg.setPublishTime(t); case _ => () }
      Success(msg)
    } else Failure(new InvalidPubsubMesage(""))

}
