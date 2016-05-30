package com.ssatapathy.streamr

import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class TwitterStreamr @Autowired()(utilities: Utilities,
                                  metrics: MetricRegistry,
                                  gauge: GaugeService)
  extends java.io.Serializable with LazyLogging {

  val counter = metrics.counter("counter.streamr.tweets")

  def stream() {
    utilities.setupTwitter()
    val ssc = getSparkStreamingContext
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText)

    computeTopHashTags(statuses, ascending = false)

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  def computeTopHashTags(statuses: DStream[String], ascending: Boolean) {
    val tweetWords = statuses.flatMap(tweet => tweet.split(" "))
    val hashTags = tweetWords.filter(tweetWord => tweetWord.startsWith("#"))
    val hashTagKeyValues = hashTags.map(hashTag => (hashTag, 1))
    val hashTagCounts = hashTagKeyValues
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
    val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print(10)
  }

  def computeTopWords(statuses: DStream[String], ascending: Boolean) {
    val tweetWords = statuses.flatMap(tweet => tweet.split(" "))
      .filter(tweetWord =>
      !List("a", "the", "to", "an", "you", "me", "i", "and", "my", "i'm", "")
        .contains(tweetWord.toLowerCase.trim))
    val tweetWordValues = tweetWords.map(tweetWord => (tweetWord, 1))
    val topWordCounts = tweetWordValues
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
    val sortedResults = topWordCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print(10)
  }


  def getSparkStreamingContext: StreamingContext = {
    new StreamingContext("local[*]", "TwitterStreamr", Seconds(1))
  }

  def saveTweets(statuses: DStream[String]) {
    var totalTweets: Long = 0
    statuses.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) {
        val repartitionedRDD = rdd.repartition(1).cache()  // rdd.coalesce(1, shuffle = false).cache()
        repartitionedRDD.saveAsTextFile("tweets_" + time.milliseconds)
        totalTweets += repartitionedRDD.count()
        println("totatTweets: " + totalTweets)
        if (totalTweets > 100) {
          System.exit(0)
        }
      }
    })
  }

  def computeAverageTweetLength(statuses: DStream[String]) {
    val totalChars = new AtomicLong(0)
    val tweetLengths = statuses.map(status => status.length)
    tweetLengths.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if(count > 0) {
        counter.inc(count)
        totalChars.addAndGet(rdd.reduce((x, y) => x + y))
        gauge.submit("streamr.avg-tweet.length", totalChars.get() / counter.getCount)
        gauge.submit("streamr.longest-tweet.length", rdd.max())
      }
    })
  }

}
