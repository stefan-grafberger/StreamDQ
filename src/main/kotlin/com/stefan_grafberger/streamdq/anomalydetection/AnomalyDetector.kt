package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

interface AnomalyDetector {
    fun detectAnomalyStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult>
    fun detectAnomalyStreamByCache(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult>
    fun detectQualifiedStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Any?>
}
