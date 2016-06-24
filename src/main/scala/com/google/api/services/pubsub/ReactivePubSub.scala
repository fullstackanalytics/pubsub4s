package com.google.api.services.pubsub

import akka.actor.ActorSystem
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import com.google.api.services.pubsub.model.PullRequest

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

class ReactivePubsub(javaPubsub: Pubsub)
                    (implicit val system: ActorSystem, val context: ExecutionContext)
  extends PublisherTrait {

  def subscribe(subscription: String, pullRequest: => model.PullRequest): Source[Seq[ReceivedMessage], NotUsed] =
    asyncRequestToSource(() => pullAsync(subscription, pullRequest))

  def subscribeConcat(subscription: String, pullRequest: => model.PullRequest): Source[ReceivedMessage, NotUsed] =
    asyncRequestToSource(() => pullAsync(subscription, pullRequest)).mapConcat{ seq => seq }

  def subscribeFromStream(subscription: String, pullRequest: => model.PullRequest): Source[Future[List[ReceivedMessage]], NotUsed] = {
    def iter(): Stream[Future[List[ReceivedMessage]]] = pullAsync(subscription, pullRequest) #:: iter()
    val res = iter().toIterator
    Source.fromIterator(() => res)
  }

  def pullAsync(subscription: String, pullRequest: => model.PullRequest): Future[List[ReceivedMessage]] =
    Future {
      // allocate additional logical thread for execution context.
      blocking {
        javaPubsub.projects().subscriptions()
          .pull(subscription, pullRequest).execute()
      }
    } map (PullResponse(_))

  def pullAsyncWithRetries(retries: Int)(subscription: String, pullRequest: => model.PullRequest)
                          (implicit system: ActorSystem) =
    retry({() => pullAsync(subscription,pullRequest)}, retries)

  def pullSync(subscription: String, pullRequest: => model.PullRequest): Try[List[ReceivedMessage]] = Try {
    PullResponse(javaPubsub.projects().subscriptions.pull(subscription, pullRequest).execute())
  }

  def pullSyncWithRetries(numRetries: Int) (subscription: String, pullRequest: => model.PullRequest) = {

    def loop(count: Int): Try[List[ReceivedMessage]] = {
      val result = pullSync(subscription, pullRequest); result match {
        case Failure(_) if count>0 => loop(count-1)
        case _ => result
      }
    }

    loop(numRetries)
  }

}

object ReactivePubsub {
  import io.fullstack.common.PortableConfiguration

  def apply(appName:String) (implicit system: ActorSystem, context: ExecutionContext): ReactivePubsub =
    apply(appName, None)(system, context)

  def apply(appName: String, url: Option[String])(implicit system: ActorSystem, context: ExecutionContext): ReactivePubsub = {
    val p: Pubsub.Builder = PortableConfiguration.createPubsubClient()
      .setApplicationName(appName)

    url match { case Some(url) => p.setRootUrl(url); case _ => () }

    new ReactivePubsub(p.build())
  }

  /** experimental for small streams. in F#, there was nice way to compose
    * sequences of asynchronous computations, and to defer execution w/in each iteration, i.e.,
    *
    * probably need Scalaz Process for that.
    *
    *  let rec pull (puller:from->RecordResult) offset = asyncSeq {
    *    let results = puller offset
    *    yield results.events |> Seq.ofArray
    *
    *    if !(results.endOfStream) then
    *    yield! pull puller results.nextEventNum
    *  }
    *
    */
  def publisherTraversal(client: ReactivePubsub, pr: PullRequest, subscription: String)
  : Future[Iterator[List[ReceivedMessage]]] = {

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

    loop() map(_.toIterator)
  }

}
