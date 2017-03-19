package com.imzhen.kafka.example.simple

/**
  * Created by Elliott on 3/15/17.
  */

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object ClickStreamSparkStreaming extends App {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: ClickStreamSparkStreaming <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    //    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val warehouseLocation = "/Users/Elliott/Desktop"

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("DirectKafkaClickStreams")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport
      .getOrCreate
    // Create context with 10 second batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream"
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaParams))
    val lines = messages.map(_.value)


    spark.sql("DROP TABLE IF EXISTS messages_hive_table")
    spark.sql("CREATE TABLE messages_hive_table ( recordTime string, eventId string, url string, ip string ) STORED AS TEXTFILE")

    // Convert RDDs of the lines DStream to DataFrame and run SQL query
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>
      import spark.implicits._
      val messagesDataFrame = rdd.map(_.split(",")).map(w => Record(w(0), w(1), w(2), w(3))).toDF()
      messagesDataFrame.createOrReplaceTempView("messages")

      spark.sql("insert into table messages_hive_table select * from messages")

      val messagesQueryDataFrame = spark.sql("select * from messages")
      println(s"========= $time =========")
      messagesQueryDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
}

case class Record(recordTime: String, eventId: String, url: String, ip: String)

// Sample Usage
// sbt "run-main com.imzhen.example.simple.kafka.ClickStreamSparkStreaming localhost:9092 test_topic"
