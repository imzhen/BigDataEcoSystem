package com.imzhen.kafka.example.twitter

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Elliott on 3/15/17.
  */
object KafkaTwitterConsumer extends App {

  val Array(brokers, group, topics, numThreads) = args

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("KafkaTwitterSample")
    .enableHiveSupport
    .getOrCreate

  spark.sparkContext.setLogLevel("WARN")

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  val checkPoint = "/Users/Elliott/CheckPoint"
  ssc.checkpoint(checkPoint)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> group
  )
  val topicsSet = topics.split(",").toSet
  val messages = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaParams))
  val lines = messages.map(_.value)
  val hashTags = lines.flatMap(_.split(" ")).filter(_.startsWith("#"))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map{ case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))
  lines.print
  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(10)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
  })

  lines.count.map(cnt => "Received " + cnt + " kafka messages.").print

  ssc.start
  ssc.awaitTermination
}

// Sample Usage:
// sbt "run-main com.imzhen.kafka.example.twitter.KafkaTwitterConsumer localhost:9092 twitter-test test_topic 2"
