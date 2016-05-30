package com.ssatapathy.streamr

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.receiver.CustomReceiver
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.regex.Matcher
import org.apache.spark.streaming._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class SessionStreamr @Autowired()(utilities: Utilities,
                                  metrics: MetricRegistry,
                                  gauge: GaugeService)
  extends java.io.Serializable with LazyLogging {

  def stream() {
    val ssc = new StreamingContext("local[*]", "SessionStreamr", Seconds(1))
    val pattern = utilities.apacheLogPattern()
    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))
    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

    // Extract the (ip, url) we want from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = scala.util.Try(requestFields(1)).getOrElse("[error]")
        (ip, url)
      } else {
        ("error", "error")
      }
    })

    val requestsWithState = requests.mapWithState(stateSpec)
    val stateSnapshotStream = requestsWithState.stateSnapshots()
    stateSnapshotStream.foreachRDD((rdd, time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.context)
      import sqlContext.implicits._
      val requestsDataFrame = rdd.map(x => (x._1, x._2.sessionLength, x._2.clickstream))
        .toDF("ip", "sessionLength", "clickstream")
      requestsDataFrame.registerTempTable("sessionData")
      val sessionsDataFrame =
        sqlContext.sql("select * from sessionData")
      println(s"========= $time =========")
      sessionsDataFrame.show()
    })

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  def trackStateFunc(batchTime: Time, ip: String, url: Option[String],
                     state: State[SessionData]): Option[(String, SessionData)] = {
    // Extract the previous state passed in (using getOrElse to handle exceptions)
    val previousState = state.getOption().getOrElse(SessionData(0, List()))

    // Create a new state that increments the session length by one,
    // adds this URL to the clickstream, and clamps the clickstream
    // list to 10 items
    val newState = SessionData(previousState.sessionLength + 1L,
      (previousState.clickstream :+ url.getOrElse("empty")).take(10))

    // Update our state with the new state.
    state.update(newState)

    // Return a new key/value result.
    Some((ip, newState))
  }

  case class SessionData(sessionLength: Long, clickstream: List[String])

}
