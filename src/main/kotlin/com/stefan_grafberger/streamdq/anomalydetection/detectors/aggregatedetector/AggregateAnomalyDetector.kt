package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.api.common.eventtime.WatermarkStrategy
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
    private var constraint: AggregateConstraint
    private var strategy: AnomalyDetectionStrategy

    init {
        this.window = window
        this.constraint = constraint
        this.strategy = strategy
    }

    override fun detectAnomalyStream(
            dataStream: SingleOutputStreamOperator<AggregateConstraintResult>
    ): SingleOutputStreamOperator<AnomalyCheckResult> {
        return strategy.detect(dataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<AggregateConstraintResult>()
                                .withTimestampAssigner { result, _ -> result.timestamp }
                )
                .windowAll(window)
                .aggregate(constraint.getAggregateFunction(dataStream.type, dataStream.executionConfig)))
    }

    override fun detectAnomalyStreamByCache(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult> {
        return strategy.apply(dataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<AggregateConstraintResult>()
                                .withTimestampAssigner { result, _ -> result.timestamp }
                )
                .windowAll(window)
                .aggregate(constraint.getAggregateFunction(dataStream.type, dataStream.executionConfig)))
    }

    override fun detectQualifiedStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Any?> {
        TODO("Not yet implemented")
    }
}
