package com.stefan_grafberger.streamdq.anomalydetection.detectors.rowdetector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectorBuilder
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class RowAnomalyDetectorBuilder : AnomalyDetectorBuilder {
    override fun build(): AnomalyDetector {
        TODO("Not yet implemented")
    }

    override fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyDetectorBuilder {
        TODO("Not yet implemented")
    }

    override fun withAggregatedConstraint(constraint: AggregateConstraint): AnomalyDetectorBuilder {
        TODO("Not yet implemented")
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyDetectorBuilder {
        TODO("Not yet implemented")
    }
}
