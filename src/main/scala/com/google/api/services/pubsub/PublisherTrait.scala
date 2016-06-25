package com.google.api.services.pubsub

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._

import scala.concurrent._

trait PublisherTrait extends RetryLogic {

  def asyncRequestToSource[Result](requester: () => Future[Result])
                                  (implicit system: ActorSystem, context: ExecutionContext): Source[Result, NotUsed] =
    asyncRequestToSource(requester, { (r: Result) => false}, 3)

  def asyncRequestToSource[Result](requester: () => Future[Result], retries: Int)
                                  (implicit system: ActorSystem, context: ExecutionContext): Source[Result, NotUsed] =
    asyncRequestToSource(requester, { (r: Result) => false}, retries)

  def asyncRequestToSource[Result](requester: () => Future[Result], cancel: Result => Boolean, retries: Int)
                                  (implicit system: ActorSystem, context: ExecutionContext): Source[Result, NotUsed] = {
    Source.unfoldAsync(()){ _ =>
      val results = retry[Result](requester, retries)
      results.map(r =>
        if (!cancel(r))
          Some((), r)
        else None)
    }
  }
}