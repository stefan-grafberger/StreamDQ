package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

interface AnomalyDetector {
    fun detectAnomalyStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult>
}
