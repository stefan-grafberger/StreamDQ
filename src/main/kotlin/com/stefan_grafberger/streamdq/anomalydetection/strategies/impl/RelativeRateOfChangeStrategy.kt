package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.RelativeRateOfChangeAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Detects anomalies based on the data's relative change(division) in the stream
 * We introduce a concept of order, which is the order of the derivative. Referring
 * to distance of two numbers that the division will be applied.
 * Finally, compare the current relative change rate with the customized rate bound.
 *
 * @param maxRateDecrease: Upper bound of accepted decrease (lower bound of increase).
 * @param maxRateIncrease: Upper bound of accepted growth.
 * @param order: order of the derivative
 */
class RelativeRateOfChangeStrategy<W : Window>(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1,
        private val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
) : AnomalyDetectionStrategy {

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(RelativeRateOfChangeAggregate(maxRateDecrease, maxRateIncrease, order))
    }
}