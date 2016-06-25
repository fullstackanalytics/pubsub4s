package com.google.api.services.pubsub

final case class PullRequest(maxMessages: Int, returnImmediately: Boolean)

object PullRequest {

  implicit def toJava(r: PullRequest): model.PullRequest = {
    new model.PullRequest()
          .setMaxMessages(r.maxMessages)
          .setReturnImmediately(r.returnImmediately)
  }
}
