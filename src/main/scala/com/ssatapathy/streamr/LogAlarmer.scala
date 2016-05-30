package com.ssatapathy.streamr

import java.util.regex.Matcher

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class LogAlarmer @Autowired()(utilities: Utilities, metrics: MetricRegistry,
                             gauge: GaugeService) extends LazyLogging {
  def stream() {
    utilities.setupTwitter()
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val statuses = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        matcher.group(6)
      } else {
        "[error]"
      }
    })
    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt).getOrElse(0)
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })
    // Tally up statuses over a 5-minute window sliding every second
    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))
    // For each batch, get the RDD's representing data from our current window
    statusCounts.foreachRDD((rdd, time) => {
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }


    println("Total success: " + totalSuccess + " Total failure: " + totalError)

    // Don't alarm unless we have some minimum amount of data to work with
    if (totalError + totalSuccess > 100) {
      // Compute the error rate
      // Note use of util.Try to handle potential divide by zero exception
      val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ).getOrElse(1.0)
      // If there are more errors than successes, wake someone up
      if (ratio > 0.5) {
        // In real life, you'd use JavaMail or Scala's courier library to send an
        // email that causes somebody's phone to make annoying noises, and you'd
        // make sure these alarms are only sent at most every half hour or something.
        println("Wake somebody up! Something is horribly wrong.")
      } else {
        println("All systems go.")
      }
    }})

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
