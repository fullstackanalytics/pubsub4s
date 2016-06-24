# pubsub4s 
WIP. Reactive Client for Google PubSub. Akka Source and serializable objects.

with emulator running...
```
import com.google.api.services.pubsub
import akka.stream._
import akka.stream.scaladsl._

implicit val system = ActorSystem("reactive-pubsub")
implicit val materializer = ActorMaterializer()

ReactivePubsub("testing", "http://localhost:8430")
  .subscribeConcat("projects/myproject/subscriptions/mysubscription")
  .map(msg => data.toString)
  .runWith(Sink.foreach(println))

```

