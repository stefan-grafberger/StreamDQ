package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions.BoundMapFunction
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * Detects anomaly based on the current element value in the data stream
 * Check if values are in a specified range.
 *
 * @param lowerBound Lower bound of accepted range of values
 * @param upperBound Upper bound of accepted range of values
 */
data class SimpleThresholdStrategy(
        val lowerBound: Double = -Double.MAX_VALUE,
        val upperBound: Double
) : AnomalyDetectionStrategy {

    init {
        require(lowerBound <= upperBound) { "The lower bound must be smaller or equal to the upper bound." }
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult> {
        val (startTimeStamp, endTimeStamp) = Pair(Long.MIN_VALUE, Long.MAX_VALUE)
        return dataStream
                .filter { data -> data.timestamp in startTimeStamp..endTimeStamp }
                .map { data -> AnomalyCheckResult(data.aggregate, false) }
                .returns(AnomalyCheckResult::class.java)
                .map(BoundMapFunction(lowerBound, upperBound))
                .returns(AnomalyCheckResult::class.java)
    }
}
