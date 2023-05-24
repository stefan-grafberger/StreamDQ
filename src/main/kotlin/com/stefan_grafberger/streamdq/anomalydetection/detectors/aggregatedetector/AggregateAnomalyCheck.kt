package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AggregateAnomalyCheck : AnomalyCheck {

    private lateinit var window: WindowAssigner<Any?, TimeWindow>
    private lateinit var constraint: AggregateConstraint
    private lateinit var strategy: AnomalyDetectionStrategy

    override fun build(): AggregateAnomalyDetector {
        return AggregateAnomalyDetector(window, constraint, strategy)
    }

    override fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck {
        this.window = windowAssigner
        return this
    }

    override fun withAggregatedConstraint(constraint: AggregateConstraint): AnomalyCheck {
        this.constraint = constraint
        return this
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck {
        this.strategy = strategy
        return this
    }
}
