package com.imzhen.kafka.example.twitter

/**
  * Created by Elliott on 3/15/17.
  */

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object KafkaTwitterProducer extends App with TwitterUtils {

  val Array(topic, keywords) = args

  final var queue = new LinkedBlockingQueue[Status](1000)
  createStream(queue, keywords)

  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "0")
  props.put("batch.size", "16384")
  props.put("linger.ms", "1")
  props.put("buffer.memory", "33554432")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  var j = 0

  while (true) {
    val ret = queue.poll()
    if (ret == null) {
      Thread.sleep(100)
    } else {
      for (hashTag <- ret.getHashtagEntities) {
        println(s"Tweet:$ret")
        println(s"HashTag: ${hashTag.getText}")
        j += 1
        producer.send(new ProducerRecord[String, String](topic, Integer.toString(j), ret.getText))
      }
    }
  }
}

trait TwitterAPI {
  val consumerKey = "VsfrFXFYdNcDAtMgChbqqIpwW"
  val consumerSecret = "tjORj0FKjrMsyu2rDkgKtzDagIYX2tu17EHJES6jJHS6Al4LD3"
  val accessToken = "3263246161-Wfzj7MwfNL5cBLUL7VLW6vE5ifugDY4TZ0lF9Qu"
  val accessTokenSecret = "L92z8KDgwanRD1PvyOqNf9i0f1dzRQjju6A8OQYyvdEoj"
}

trait TwitterUtils extends TwitterAPI {
  def createStream(queue: LinkedBlockingQueue[Status], keywords: String) = {
    val cb = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
    val listener = new StatusListener {
      override def onStallWarning(warning: StallWarning) = {}

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = {}

      override def onScrubGeo(userId: Long, upToStatusId: Long) = {}

      override def onStatus(status: Status) = {
        queue.offer(status)
      }

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int) = {}

      override def onException(ex: Exception) = {
        ex.printStackTrace()
      }
    }
    twitterStream.addListener(listener)

    val query = new FilterQuery().track(keywords)
    twitterStream.filter(query)
  }
}

// Sample Usage:
// sbt "run-main com.imzhen.kafka.example.twitter.KafkaTwitterProducer test_topic"
