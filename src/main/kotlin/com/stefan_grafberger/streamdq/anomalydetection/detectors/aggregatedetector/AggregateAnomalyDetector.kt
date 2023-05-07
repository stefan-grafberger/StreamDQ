package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import org.apache.flink.streaming.api.datastream.DataStream

class AggregateAnomalyDetector : AnomalyDetector {
    override fun detectAnomalyStream(): DataStream<Any?> {
        TODO("Not yet implemented")
    }

    override fun detectQualifiedStream(): DataStream<Any?> {
        TODO("Not yet implemented")
    }
}
