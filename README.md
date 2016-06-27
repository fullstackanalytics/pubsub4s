# pubsub4s 
Reactive client wrapper for Google PubSub java library. 

For use with Akka via custom `Source` and removal of `null`s which are not supported in Akka.

## Usage
```
import com.google.api.services.pubsub
import akka.stream._
import akka.stream.scaladsl._

implicit val system = ActorSystem("reactive-pubsub")
implicit val materializer = ActorMaterializer()

val pullRequest = PullRequest(10, false)
ReactivePubsub("testing", "http://localhost:8430") // Emulator at locahost.
  .subscribeConcat("projects/myproject/subscriptions/mysubscription", pullRequest)
  .map(msg => data.toString)
  .runWith(Sink.foreach(println))
```

## Testing

Use emulator on locahost.

