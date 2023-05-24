package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

interface AnomalyDetector {
    fun <IN> detectAnomalyStream(dataStream: DataStream<IN>): SingleOutputStreamOperator<AnomalyCheckResult>
}
