package com.google.api.services.pubsub

import akka.actor.ActorSystem
import akka.NotUsed
import akka.stream.scaladsl._
import utils.PortableConfiguration

import scala.concurrent._
import scala.util.{Failure, Try}
import scala.language.implicitConversions._

class ReactivePubsub(javaPubsub: Pubsub) extends PublisherTrait {

  def subscribe
      (subscription: String, pullRequest: PullRequest)
      (implicit ec: ExecutionContext, s: ActorSystem): Source[Seq[ReceivedMessage], NotUsed] =
    asyncRequestToSource(() => pullAsync(subscription, pullRequest))

  def subscribeConcat
      (subscription: String, pullRequest: PullRequest)
      (implicit ec: ExecutionContext, s: ActorSystem): Source[ReceivedMessage, NotUsed] =
    asyncRequestToSource(() => pullAsync(subscription, pullRequest)).mapConcat{ seq => seq }

  def subscribeFromStream
      (subscription: String, pullRequest: PullRequest)
      (implicit ec: ExecutionContext, s: ActorSystem): Source[Future[List[ReceivedMessage]], NotUsed] = {

    def iter(): Stream[Future[List[ReceivedMessage]]] =
      pullAsync(subscription, pullRequest) #:: iter()

    val res = iter().toIterator
    Source.fromIterator(() => res)
  }

  // allocate additional logical thread for execution context.
  def pullAsync
      (subscription: String, pullRequest: PullRequest)
      (implicit ec: ExecutionContext, s: ActorSystem): Future[List[ReceivedMessage]] =
    Future {
      blocking {
        javaPubsub.projects().subscriptions()
          .pull(subscription, pullRequest).execute()
      }
    } map (PullResponse(_))

  def pullAsyncWithRetries
      (retries: Int)(subscription: String, pullRequest: PullRequest)
      (implicit ec: ExecutionContext, system: ActorSystem) =
    retry({() => pullAsync(subscription, pullRequest)}, retries)

  def pullSync(subscription: String, maxMessages: Int, returnImmediately: Boolean): Try[List[ReceivedMessage]] = Try {
    PullResponse(javaPubsub.projects().subscriptions.pull(subscription, PullRequest(maxMessages, returnImmediately)).execute())
  }

  def pullSyncWithRetries(numRetries: Int) (subscription: String, maxMessages: Int, returnImmediately: Boolean) = {

    def loop(count: Int): Try[List[ReceivedMessage]] = {
      val result = pullSync(subscription, maxMessages, returnImmediately); result match {
        case Failure(_) if count>0 => loop(count-1)
        case _ => result
      }
    }

    loop(numRetries)
  }

}

object ReactivePubsub {

  def apply(appName:String) (implicit system: ActorSystem, context: ExecutionContext): ReactivePubsub =
    apply(appName, None)(system, context)

  def apply
      (appName: String, url: Option[String])
      (implicit system: ActorSystem, context: ExecutionContext): ReactivePubsub = {

    val p: Pubsub.Builder = PortableConfiguration.createPubsubClient()
      .setApplicationName(appName)

    url match { case Some(u) => p.setRootUrl(u); case _ => () }

    new ReactivePubsub(p.build())
  }

  // read all until at head of stream. Warning, execution commences immediately.
  def readAll
      (client: ReactivePubsub, pr: PullRequest, subscription: String)
      (implicit system: ActorSystem, context: ExecutionContext): Future[Stream[ReceivedMessage]] = {

    def loop(): Future[Stream[List[ReceivedMessage]]] =
      for {
        result <- client.pullAsync(subscription, pr)
        size    = result.length
        next   <- loop()
      } yield {
        if (size > 0)
          result #:: next
        else
          Stream.empty[List[ReceivedMessage]]
      }

    loop() map(x => x.flatMap(_.toStream))
  }

}
