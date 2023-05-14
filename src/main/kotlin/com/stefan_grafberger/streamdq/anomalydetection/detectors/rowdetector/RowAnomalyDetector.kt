package com.stefan_grafberger.streamdq.anomalydetection.detectors.rowdetector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

class RowAnomalyDetector : AnomalyDetector {
    override fun detectAnomalyStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        TODO("Not yet implemented")
    }

    override fun detectQualifiedStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Any?> {
        TODO("Not yet implemented")
    }
}
