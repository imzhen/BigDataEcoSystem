package com.imzhen.kafka.example.akka

import java.util.Date

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Elliott on 3/16/17.
  */

object Stream extends App {

  val Array(events, topic, brokers) = args

  implicit val system = ActorSystem.create("akka-stream-kafka-test")
  implicit val materializer = ActorMaterializer()

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(brokers)
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(brokers)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerGraphSimple = Source(0 until events.toInt)
    .map { n =>
      val partition = 0
      val runtime = new Date().getTime
      val ip = "192.168.2." + n
      val msg = runtime + "," + n + ",www.example.com," + ip
      new ProducerRecord[Array[Byte], String](topic, partition, null, msg)
    }
    .to(Producer.plainSink(producerSettings))
  //  OR
  val producerGraph = Source(0 until events.toInt)
    .map { n =>
      val partition = 0
      val runtime = new Date().getTime
      val ip = "192.168.2." + n
      val msg = runtime + "," + n + ",www.example.com," + ip
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
        topic, partition, null, msg
      ), n)
    }
    .via(Producer.flow(producerSettings))
    .to(Sink.ignore)

  val consumerGraphSimple = Consumer.plainSource(consumerSettings, Subscriptions.topics("test_topic"))
    .mapAsync(1){ msg =>
      println(msg)
      Future.successful(msg)
    }
    .to(Sink.ignore)
  // OR
  val consumerGraph = Consumer.committableSource(consumerSettings, Subscriptions.topics("test_topic"))
    .mapAsync(1){msg =>
      msg.committableOffset.commitScaladsl()
    }
    .to(Sink.ignore)

  consumerGraphSimple.run()
  producerGraph.run()

  system.scheduler.scheduleOnce(5.seconds){
    system.terminate()
  }

}

// Sample Usage
// sbt "run-main com.imzhen.kafka.example.akka.Stream 10000 test_topic localhost:9092"
