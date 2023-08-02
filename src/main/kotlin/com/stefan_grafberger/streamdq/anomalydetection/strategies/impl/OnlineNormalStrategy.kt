package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.OnlineNormalAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Detects anomaly based on the currently running mean and standard deviation
 * Assume the data is normal distributed
 *
 * @param lowerDeviationFactor   Catch anomalies if the data stream has a mean
 *                               smaller than mean - lowerDeviationFactor * stdDev
 * @param upperDeviationFactor   Catch anomalies if the data stream has a mean
 *                               bigger than mean + upperDeviationFactor * stdDev
 * @param strategyWindowAssigner Flink window assigner used for windowing the data stream
 *                               and perform aggregate computation to detect anomalies
 *                               based on windowed stream
 */
data class OnlineNormalStrategy<W : Window>(
        val lowerDeviationFactor: Double? = 3.0,
        val upperDeviationFactor: Double? = 3.0,
        val ignoreAnomalies: Boolean = true,
        val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
) : AnomalyDetectionStrategy {

    init {
        require(lowerDeviationFactor != null || upperDeviationFactor != null) { "At least one factor has to be specified." }
        require(
                (lowerDeviationFactor ?: 1.0) >= 0 && (upperDeviationFactor
                        ?: 1.0) >= 0
        ) { "Factors cannot be smaller than zero." }
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>)
            : SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(OnlineNormalAggregate(lowerDeviationFactor, upperDeviationFactor))
                .map { data -> AnomalyCheckResult(data.value, data.isAnomaly) }
                .returns(AnomalyCheckResult::class.java)
    }
}
