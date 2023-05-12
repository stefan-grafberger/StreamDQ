package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
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

    override fun detectAnomalyStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Anomaly> {
        return strategy.apply(dataStream
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .trigger(CountTrigger.of(10))
                .aggregate(constraint.getAggregateFunction(dataStream.type, dataStream.executionConfig)))
    }

    override fun detectQualifiedStream(dataStream: DataStream<Any?>): SingleOutputStreamOperator<Any?> {
        TODO("Not yet implemented")
    }
}
