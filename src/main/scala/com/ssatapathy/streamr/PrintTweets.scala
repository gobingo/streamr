package com.ssatapathy.streamr

import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class PrintTweets @Autowired()(utilities: Utilities) extends LazyLogging {

  def fetchTweets() {
    utilities.setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText)

    // Print out the first ten
    statuses.print()

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }

}
