name := "KafkaTest"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"
val kafkaVersion = "0.10.2.0"
val twitterVersion = "4.0.6"
val akkaStreamKafkaVersion = "0.14"
val akkaVersion = "2.4.17"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.twitter4j" % "twitter4j-core" % twitterVersion,
  "org.twitter4j" % "twitter4j-stream" % twitterVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion
)
