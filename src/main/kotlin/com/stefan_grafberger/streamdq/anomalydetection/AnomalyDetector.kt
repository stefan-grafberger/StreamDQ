package com.stefan_grafberger.streamdq.anomalydetection

import org.apache.flink.streaming.api.datastream.DataStream

interface AnomalyDetector {
    fun detectAnomalyStream(): DataStream<Any?>
    fun detectQualifiedStream(): DataStream<Any?>
}
