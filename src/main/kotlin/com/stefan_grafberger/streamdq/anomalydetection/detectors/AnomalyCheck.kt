package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.model.metrics.Metric
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

interface AnomalyCheck {
    fun build(): AnomalyDetector
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck
    fun withMetric(metric: Metric, keyExpressionString: String): AnomalyCheck
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck
}