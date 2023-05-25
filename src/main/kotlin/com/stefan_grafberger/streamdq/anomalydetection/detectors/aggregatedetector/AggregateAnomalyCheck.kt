package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.*
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

    override fun onCompleteness(keyExpressionString: String): AnomalyCheck {
        this.metric = CompletenessConstraint(keyExpressionString)
        return this
    }

    override fun onApproxUniqueness(keyExpressionString: String): AnomalyCheck {
        this.metric = ApproxUniquenessConstraint(keyExpressionString)
        return this
    }

    override fun onApproxCountDistinct(keyExpressionString: String): AnomalyCheck {
        this.metric = ApproxCountDistinctConstraint(keyExpressionString)
        return this
    }

    override fun onApproxQuantileConstraint(keyExpressionString: String, quantile: Double): AnomalyCheck {
        this.metric = ApproxQuantileConstraint(keyExpressionString, quantile)
        return this
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck {
        this.strategy = strategy
        return this
    }
}
