package com.google.api.services.pubsub

import akka.actor.ActorSystem
import akka.pattern.after

import scala.concurrent._
import scala.concurrent.duration._

trait RetryLogic {

  def retry[B](op: () => Future[B], retries: Int)(implicit ec: ExecutionContext, s: ActorSystem): Future[B] =
    op() recoverWith {
      case _ if retries > 0 => after(2 seconds, using = s.scheduler)(retry(op, retries - 1))
    }

  def retry[A,B](op: A => Future[B], retries: Int)(elem: A)(implicit ec: ExecutionContext, s: ActorSystem): Future[B] =
    op(elem) recoverWith {
      case _ if retries > 0 => after(2 seconds, using = s.scheduler)(retry(op, retries - 1)(elem))
    }

}
