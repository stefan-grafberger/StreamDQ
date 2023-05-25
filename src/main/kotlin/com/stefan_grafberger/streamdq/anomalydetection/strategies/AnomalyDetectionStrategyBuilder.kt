package com.stefan_grafberger.streamdq.anomalydetection.strategies

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window

interface AnomalyDetectionStrategyBuilder <W:Window> {
    fun withWindow(windowAssigner: WindowAssigner<Any?, W>) : AnomalyDetectionStrategyBuilder<W>
    fun build() : AnomalyDetectionStrategy
}