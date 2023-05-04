package com.stefan_grafberger.streamdq.anomalydetection.detector

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.Check
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import com.stefan_grafberger.streamdq.checks.aggregate.WindowAggregateCheck
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window

abstract class AnomalyCheck : Check() {

    fun <W : Window> onWindow(aggregateWindowAssigner: WindowAssigner<Any?, W>)
            : WindowAggregateCheck<W> {
        return WindowAggregateCheck(aggregateWindowAssigner)
    }

    fun <T> onAggregateMetric(anomalyDetectionStrategy: AnomalyDetectionStrategy,
                              streamObjectTypeInfo: TypeInformation<T>, config: ExecutionConfig?)
            : TypeQueryableAggregateFunction<T> {
        return anomalyDetectionStrategy.getAggregateFunction(streamObjectTypeInfo, config)
    }
}