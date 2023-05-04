package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.*
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * The common interface for all anomaly detection strategies
 */
interface AnomalyDetectionStrategy {
    fun detect(dataStream: List<Double>, searchInterval: Pair<Int, Int>)
            : MutableCollection<Pair<Int, Anomaly>>

    /**
     * getAggregateFunction: return exact object, aggregation function
     * from flink. to look: HllSketchCountDistinctAggregateTest
     */
    fun <R> getAggregateFunction(): SingleOutputStreamOperator<R>

    fun <T> getAggregateFunction(streamObjectTypeInfo: TypeInformation<T>, config: ExecutionConfig?
    ): TypeQueryableAggregateFunction<T>

    /**
     * getWindow: addWindowOrTriggerKeyed also non-keyed
     */
    fun <IN, KEY> addWindowOrTriggerKeyed(keyedStream: KeyedStream<IN, KEY>)
            : WindowedStream<IN, KEY, Window>

    fun <IN> addWindowOrTriggerNonKeyed(dataStream: DataStream<IN>, mergeKeyedResultsOnly: Boolean)
            : AllWindowedStream<IN, Window>
}