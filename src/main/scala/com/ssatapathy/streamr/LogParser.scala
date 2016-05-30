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
class LogParser @Autowired()(utilities: Utilities, metrics: MetricRegistry,
                             gauge: GaugeService) extends LazyLogging {
  def stream() {
    utilities.setupTwitter()
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        matcher.group(5)
      }
    })
    val urls = requests.map(x => {
      val arr = x.toString.split(" ")
      if (arr.size == 3) {
        arr(1)
      } else {
        "[error]"
      }})
    val urlCounts = urls.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
