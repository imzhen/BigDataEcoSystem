/**
  * Created by Elliott on 3/14/17.
  */

package com.imzhen.kafka.example.simple

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ScalaProducerExample extends App {
  val events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  val rnd = new Random()
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime
    val ip = "192.168.2." + nEvents
    val msg = runtime + "," + nEvents + ",www.example.com," + ip
    val data = new ProducerRecord[String, String](topic, ip, msg)

    producer.send(data)
  }

  println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}

// Sample Usage
// sbt "run-main com.imzhen.kafka.example.simple.ScalaProducerExample 10000 test_topic localhost:9092"
