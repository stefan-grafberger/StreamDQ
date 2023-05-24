package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.model.metrics.Metric
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
    private lateinit var metric: AggregateConstraint

    override fun build(): AggregateAnomalyDetector {
        return AggregateAnomalyDetector(window, metric, strategy)
    }

    override fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck {
        this.window = windowAssigner
        return this
    }

    override fun withMetric(metric: Metric, keyExpressionString: String): AnomalyCheck {
        this.metric = when (metric) {
            Metric.COMPLETENESS ->
                CompletenessConstraint(keyExpressionString)

            Metric.APPROX_UNIQUENESS ->
                ApproxUniquenessConstraint(keyExpressionString)

            Metric.APPROX_COUNT_DISTINCT ->
                ApproxCountDistinctConstraint(keyExpressionString)
        }
        return this
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck {
        this.strategy = strategy
        return this
    }
}
