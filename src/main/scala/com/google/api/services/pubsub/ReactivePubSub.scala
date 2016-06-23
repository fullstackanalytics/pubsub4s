package com.google.api.services.pubsub

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import PullResponse.{PullResponse => SerailizablePullResponse}
import akka.actor.ActorSystem

import scala.util.{Failure, Success, Try}

class ReactivePubsub(javaPubsub: Pubsub) extends RetryLogic {

  def pullAsync(subscription: String, pullRequest: => model.PullRequest): Future[SerailizablePullResponse] =
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

  def pullSync(subscription: String, pullRequest: => model.PullRequest): Try[SerailizablePullResponse] = Try {
    PullResponse(javaPubsub.projects().subscriptions.pull(subscription, pullRequest).execute())
  }

  def pullSyncNoTry(subscription: String, pullRequest: => model.PullRequest): SerailizablePullResponse =
    PullResponse(javaPubsub.projects().subscriptions.pull(subscription, pullRequest).execute())

  def pullSyncWithRetries(numRetries: Int) (subscription: String, pullRequest: => model.PullRequest) = {

    def loop(count: Int): Try[SerailizablePullResponse] = {
      val result = pullSync(subscription, pullRequest); result match {
        case Failure(_) if count>0 => loop(count-1)
        case _ => result
      }
    }

    loop(numRetries)
  }

}

object ReactivePubsub {

  def apply(url: String): ReactivePubsub = {
    val p: Pubsub = ???
    new ReactivePubsub(p)
  }

}