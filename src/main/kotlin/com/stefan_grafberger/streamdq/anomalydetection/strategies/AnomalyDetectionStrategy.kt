package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The common interface for all anomaly detection strategies
 * Override in [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl]
 *
 * @since 1.0
 */
interface AnomalyDetectionStrategy {
    fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>):
            SingleOutputStreamOperator<AnomalyCheckResult>
}
