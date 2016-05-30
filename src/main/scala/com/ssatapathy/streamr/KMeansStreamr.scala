package com.ssatapathy.streamr

import com.codahale.metrics.MetricRegistry
import com.ssatapathy.streamr.receiver.CustomReceiver
import com.ssatapathy.streamr.utils.Utilities
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.stereotype.Service

@Service
class KMeansStreamr @Autowired()(utilities: Utilities,
                                 metrics: MetricRegistry,
                                 gauge: GaugeService)
  extends java.io.Serializable with LazyLogging {

  def stream() {
    val ssc = new StreamingContext("local[*]", "KMeansStreamr", Seconds(1))
    val trainingStream = ssc.receiverStream(new CustomReceiver("localhost", 9999))
    val testingStream = ssc.receiverStream(new CustomReceiver("localhost", 7777))
    val trainingData = trainingStream.map(Vectors.parse).cache()
    val testData = testingStream.map(LabeledPoint.parse)

    trainingData.print()

    // Build a K-Means clustering model for 5 clusters and 2 features (age and income)
    val model = new StreamingKMeans()
      .setK(5)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label.toInt, lp.features))).print()

    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
