package com.ssatapathy.streamr

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.regex.Matcher
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class KafkaStreamr @Autowired()(utilities: Utilities,
                                metrics: MetricRegistry,
                                gauge: GaugeService) extends java.io.Serializable with LazyLogging {

  def stream() {
    val ssc = new StreamingContext("local[*]", "KafkaStreamr", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("testLogs").toSet
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

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
