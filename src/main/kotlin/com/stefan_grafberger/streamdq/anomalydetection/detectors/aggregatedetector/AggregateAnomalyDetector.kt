package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AggregateAnomalyDetector(
        window: WindowAssigner<Any?, TimeWindow>,
        constraint: AggregateConstraint,
        strategy: AnomalyDetectionStrategy
) : AnomalyDetector {

    private var window: WindowAssigner<Any?, TimeWindow>
    var constraint: AggregateConstraint
    var strategy: AnomalyDetectionStrategy

    init {
        this.window = window
        this.constraint = constraint
        this.strategy = strategy
    }

    override fun <IN> detectAnomalyStream(
            dataStream: DataStream<IN>
    ): SingleOutputStreamOperator<AnomalyCheckResult> {
        return strategy.detect(dataStream
                .windowAll(window)
                .aggregate(constraint.getAggregateFunction(dataStream.type, dataStream.executionConfig)))
    }
}
