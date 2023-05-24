package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

interface AnomalyCheck {
    fun build(): AnomalyDetector
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck
    fun withAggregatedConstraint(constraint: AggregateConstraint): AnomalyCheck
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck
}
