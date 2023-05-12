package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

interface AnomalyDetector {
    fun detectAnomalyStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Anomaly>
    fun detectQualifiedStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Any?>
}
