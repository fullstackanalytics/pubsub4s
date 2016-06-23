package com.google.api.services.pubsub.publishers

import com.google.api.services.pubsub.PullResponse.{PullResponse => SerializedPullResponse}
import com.google.api.services.pubsub.model.PullRequest
import com.google.api.services.pubsub.{ReactivePubsub, ReceivedMessage}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Success, Try}

/**
  * Will need to test if using Sequence of Futures in Akka Source is more effective
  * (less context switching) than calling blocking calls in source.
  */
object IteratorUtils {

  def publisher(client: ReactivePubsub, pr: PullRequest) (subscription: String) =
    Stream.iterate(List.empty[ReceivedMessage])(x => client.pullSyncNoTry(subscription, pr))
      .map(Try(_))
      .drop(1)

  def filteredPublisher (c: ReactivePubsub, pr: PullRequest) (s: String) =
    publisher(c, pr)(s) flatMap { t => t match { case Success(resp) => resp; case _ => List.empty[ReceivedMessage] }}

  def publisherAsync(client: ReactivePubsub, pr: PullRequest) (subscription: String) = {
    def iter(): Stream[Future[SerializedPullResponse]] = client.pullAsync(subscription, pr) #:: iter()
    iter()
  }

  def publisherSync(client: ReactivePubsub, pr: PullRequest) (subscription: String) =
    publisherSyncWithRetries(client, pr) (subscription, 3)

  def publisherSyncWithRetries(client: ReactivePubsub, pr: PullRequest) (subscription: String, retries: Int) = {
    def iter(): Stream[Try[SerializedPullResponse]] = client.pullSyncWithRetries(retries)(subscription, pr) #:: iter()
    iter()
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
  : Future[Iterator[SerializedPullResponse]] = {

    def loop(): Future[Stream[SerializedPullResponse]] =
      for {
        result <- client.pullAsync(subscription, pr)
        size    = result.length
        next   <- loop()
      } yield {
        if (size > 0)
          result #:: next
        else
          Stream.empty[SerializedPullResponse]
      }

    loop() map(_.toIterator)
  }


}
