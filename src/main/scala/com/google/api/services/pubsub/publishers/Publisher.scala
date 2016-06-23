package com.google.api.services.pubsub.publishers

import akka.stream._
import akka.stream.scaladsl._
import com.google.api.services.pubsub.{ReactivePubsub, ReceivedMessage}
import com.google.api.services.pubsub.model.PullRequest

object Publisher {

  def publisher(client: ReactivePubsub, pr: PullRequest)(subscription: String) = {
    val t = IteratorUtils.filteredPublisher(client, pr)(subscription)
    //Source[ReceivedMessage](t.to[collection.immutable.Iterable])
    Source.fromIterator(() => t.toIterator)
  }

}
