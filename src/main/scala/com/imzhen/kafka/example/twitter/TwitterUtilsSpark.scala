package com.imzhen.kafka.example.twitter

/**
  * Created by Elliott on 3/15/17.
  */

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{Status, _}

object TwitterUtilsSpark {

  def createStream(jssc: JavaStreamingContext): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None)
  }

  def createStream(
                    ssc: StreamingContext,
                    twitterAuth: Option[Authorization],
                    filters: Seq[String] = Nil,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[Status] = {
    new TwitterInputDStream(ssc, twitterAuth, filters, storageLevel)
  }

  def createStream(jssc: JavaStreamingContext, filters: Array[String]
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, filters)
  }

  def createStream(
                    jssc: JavaStreamingContext,
                    filters: Array[String],
                    storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, None, filters, storageLevel)
  }

  def createStream(jssc: JavaStreamingContext, twitterAuth: Authorization
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth))
  }

  def createStream(
                    jssc: JavaStreamingContext,
                    twitterAuth: Authorization,
                    filters: Array[String]
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), filters)
  }

  def createStream(
                    jssc: JavaStreamingContext,
                    twitterAuth: Authorization,
                    filters: Array[String],
                    storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[Status] = {
    createStream(jssc.ssc, Some(twitterAuth), filters, storageLevel)
  }
}

private class TwitterInputDStream(
                           ssc_ : StreamingContext,
                           twitterAuth: Option[Authorization],
                           filters: Seq[String],
                           storageLevel: StorageLevel
                         ) extends ReceiverInputDStream[Status](ssc_) {

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  override def getReceiver(): Receiver[Status] = {
    new TwitterReceiver(authorization, filters, storageLevel)
  }

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }
}

private class TwitterReceiver(
                       twitterAuth: Authorization,
                       filters: Seq[String],
                       storageLevel: StorageLevel
                     ) extends Receiver[Status](storageLevel) {

  @volatile private var twitterStream: TwitterStream = _
  @volatile private var stopped = false

  def onStart() {
    try {
      val newTwitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
      newTwitterStream.addListener(new StatusListener {
        def onStatus(status: Status): Unit = {
          store(status)
        }

        // Unimplemented
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

        def onTrackLimitationNotice(i: Int) {}

        def onScrubGeo(l: Long, l1: Long) {}

        def onStallWarning(stallWarning: StallWarning) {}

        def onException(e: Exception) {
          if (!stopped) {
            restart("Error receiving tweets", e)
          }
        }
      })

      val query = new FilterQuery
      if (filters.nonEmpty) {
        query.track(filters.mkString(","))
        newTwitterStream.filter(query)
      } else {
        newTwitterStream.sample()
      }
      setTwitterStream(newTwitterStream)
      stopped = false
    } catch {
      case e: Exception => restart("Error starting Twitter stream", e)
    }
  }

  private def setTwitterStream(newTwitterStream: TwitterStream) = synchronized {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
    twitterStream = newTwitterStream
  }

  def onStop() {
    stopped = true
    setTwitterStream(null)
  }
}