package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * The interface for building AnomalyDetector
 * It is used for adding all the needed configurations
 * when user define anomaly check before using it in the
 * [com.stefan_grafberger.streamdq.VerificationSuite]
 *
 * @Override by [com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector]
 *
 * @since 1.0
 */
interface AnomalyCheck {
    fun build(): AnomalyDetector
    fun onCompleteness(keyExpressionString: String): AnomalyCheck
    fun onApproxUniqueness(keyExpressionString: String): AnomalyCheck
    fun onApproxCountDistinct(keyExpressionString: String): AnomalyCheck
    fun onApproxQuantile(keyExpressionString: String, quantile: Double): AnomalyCheck
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck
}
