package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.AbsoluteChangeAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window

class AbsoluteChangeStrategy<W : Window>(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1,
        private val strategyWindowAssigner: WindowAssigner<Any?, W>? = null) : AnomalyDetectionStrategy {

    init {
        require(maxRateDecrease <= maxRateIncrease) {
            "The maximal rate of increase has to be bigger than the maximal rate of decrease."
        }
        require(order >= 0) { "Order of derivative cannot be negative." }
    }

    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, AnomalyCheckResult>> {
        TODO("Not yet implemented")
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>, waterMarkInterval: Pair<Long, Long>?): SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(AbsoluteChangeAggregate(maxRateDecrease, maxRateIncrease, order))
    }
}
