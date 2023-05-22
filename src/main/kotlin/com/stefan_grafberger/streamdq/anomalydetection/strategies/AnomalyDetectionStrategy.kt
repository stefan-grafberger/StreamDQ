package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The common interface for all anomaly detection strategies
 */
interface AnomalyDetectionStrategy {
    fun detect(cachedStream: List<Double>,
               searchInterval: Pair<Int, Int> = Pair(0, cachedStream.size))
            : MutableCollection<Pair<Int, AnomalyCheckResult>>

    fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>,
               waterMarkInterval: Pair<Long, Long>?=null): SingleOutputStreamOperator<AnomalyCheckResult>

    fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult>

    fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>,
              searchInterval: Pair<Int, Int>): SingleOutputStreamOperator<AnomalyCheckResult>
}
