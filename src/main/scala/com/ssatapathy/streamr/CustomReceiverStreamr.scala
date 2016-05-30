package com.ssatapathy.streamr

import java.util.regex.Matcher

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.receiver.CustomReceiver
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class CustomReceiverStreamr @Autowired()(utilities: Utilities, metrics: MetricRegistry,
                                         gauge: GaugeService) extends LazyLogging {

  def stream() {
    val ssc = new StreamingContext("local[*]", "CustomReceiverStreamr", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

    val urls = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        if (requestFields.size == 3) {
          requestFields(1)
        }
      } else {
        "[error]"
      }
    })

    val urlCounts = urls.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    sortedResults.print()

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
