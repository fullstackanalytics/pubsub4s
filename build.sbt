name := "ReactivePubSub"
organization := "io.fullstackanalytics"
version := "0.1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.7",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.7",
  "com.typesafe.akka" %% "akka-stream" % "2.4.7",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.7",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.7",
  "com.google.apis" % "google-api-services-pubsub" % "v1-rev11-1.22.0",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
