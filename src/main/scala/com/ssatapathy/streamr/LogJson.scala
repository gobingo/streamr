package com.ssatapathy.streamr

import java.util.regex.Matcher

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class LogJson @Autowired()(utilities: Utilities, metrics: MetricRegistry,
                           gauge: GaugeService) extends LazyLogging {

  def stream() {
    val ssc = new StreamingContext("local[*]", "LogJson", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = util.Try(requestFields(1)).getOrElse("[error]")
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })

    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()
      if (requestsDataFrame.count() > 0) {
        requestsDataFrame.write.mode(SaveMode.Overwrite).json("requests")
      }
    })

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
