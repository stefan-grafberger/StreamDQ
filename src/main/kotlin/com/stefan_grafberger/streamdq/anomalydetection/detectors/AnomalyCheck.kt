package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

interface AnomalyCheck {
    fun build(): AnomalyDetector
    fun onCompleteness(keyExpressionString: String): AnomalyCheck
    fun onApproxUniqueness(keyExpressionString: String): AnomalyCheck
    fun onApproxCountDistinct(keyExpressionString: String): AnomalyCheck
    fun onApproxQuantile(keyExpressionString: String, quantile: Double): AnomalyCheck
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck
}
