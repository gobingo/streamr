package com.ssatapathy.streamr

import com.codahale.metrics.MetricRegistry
import com.datastax.spark.connector._
import com.ssatapathy.streamr.receiver.CustomReceiver
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.regex.Matcher
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class CassandraStreamr @Autowired()(utilities: Utilities, metrics: MetricRegistry,
                                    gauge: GaugeService) extends LazyLogging {

  def stream() {
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("CassandraStreamr")
    val ssc = new StreamingContext(conf, Seconds(10))
    val pattern = utilities.apacheLogPattern()
    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

    // Extract the (IP, URL, status, useragent) tuples that match our schema in Cassandra
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = scala.util.Try(requestFields(1)).getOrElse("[error]")
        (ip, url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", "error", 0, "error")
      }
    })

    requests.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra("ssatapathy", "log_test", SomeColumns("ip", "url", "status", "user_agent"))
    })

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
