package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

interface AnomalyDetectorBuilder {
    fun build(): AnomalyDetector
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyDetectorBuilder
    fun withAggregatedConstraint(constraint: AggregateConstraint): AnomalyDetectorBuilder
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyDetectorBuilder
}
