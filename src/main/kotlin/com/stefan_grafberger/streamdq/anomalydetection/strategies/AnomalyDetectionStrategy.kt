package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The common interface for all anomaly detection strategies
 */
interface AnomalyDetectionStrategy {
    fun detectOnCache(cachedStream: List<Double>,
               searchInterval: Pair<Int, Int> = Pair(0, cachedStream.size))
            : MutableCollection<Pair<Int, Anomaly>>

    fun detectOnStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly>

    fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly>

    fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>,
              searchInterval: Pair<Int, Int>): SingleOutputStreamOperator<Anomaly>
}
