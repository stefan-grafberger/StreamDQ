package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.model.metrics.Metrics
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import com.stefan_grafberger.streamdq.checks.aggregate.ApproxCountDistinctConstraint
import com.stefan_grafberger.streamdq.checks.aggregate.ApproxUniquenessConstraint
import com.stefan_grafberger.streamdq.checks.aggregate.CompletenessConstraint
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AggregateAnomalyCheck : AnomalyCheck {

    private lateinit var window: WindowAssigner<Any?, TimeWindow>
    private lateinit var strategy: AnomalyDetectionStrategy
    private lateinit var metrics: AggregateConstraint

    override fun build(): AggregateAnomalyDetector {
        return AggregateAnomalyDetector(window, metrics, strategy)
    }

    override fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck {
        this.window = windowAssigner
        return this
    }

    override fun withMetrics(metrics: Metrics, keyExpressionString: String): AnomalyCheck {
        this.metrics = when (metrics) {
            Metrics.COMPLETENESS ->
                CompletenessConstraint(keyExpressionString)

            Metrics.APPROX_UNIQUENESS ->
                ApproxUniquenessConstraint(keyExpressionString)

            Metrics.APPROX_COUNT_DISTINCT ->
                ApproxCountDistinctConstraint(keyExpressionString)
        }
        return this
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck {
        this.strategy = strategy
        return this
    }
}
