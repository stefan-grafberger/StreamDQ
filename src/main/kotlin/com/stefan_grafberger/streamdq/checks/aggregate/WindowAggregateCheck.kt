package com.stefan_grafberger.streamdq.checks.aggregate

import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window

data class WindowAggregateCheck<W : Window>(val aggregateWindowAssigner: WindowAssigner<Any?, W>) : InternalAggregateCheck() {
    override fun <IN, KEY> addWindowOrTriggerKeyed(accessedfieldStream: KeyedStream<IN, KEY>): WindowedStream<IN, KEY, Window> {
        val windowedStream = accessedfieldStream.window(this.aggregateWindowAssigner)
        @Suppress("UNCHECKED_CAST")
        return windowedStream as WindowedStream<IN, KEY, Window>
    }

    override fun <IN> addWindowOrTriggerNonKeyed(
        accessedfieldStream: DataStream<IN>,
        mergeKeyedResultsOnly: Boolean
    ): AllWindowedStream<IN, Window> {
        val windowedStream = accessedfieldStream.windowAll(this.aggregateWindowAssigner)
        @Suppress("UNCHECKED_CAST")
        return windowedStream as AllWindowedStream<IN, Window>
    }

    fun hasCompletenessBetween(
        keyExpressionString: String,
        expectedLowerBound: Double? = null,
        expectedUpperBound: Double? = null
    ): WindowAggregateCheck<W> {
        this.constraints.add(CompletenessConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxCountDistinctBetween(
        keyExpressionString: String,
        expectedLowerBound: Int? = null,
        expectedUpperBound: Int? = null
    ): WindowAggregateCheck<W> {
        this.constraints.add(ApproxCountDistinctConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxUniquenessBetween(
        keyExpressionString: String,
        expectedLowerBound: Double? = null, // TODO: Non-Double Types
        expectedUpperBound: Double? = null
    ): WindowAggregateCheck<W> {
        this.constraints.add(ApproxUniquenessConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxQuantileBetween(
        keyExpressionString: String,
        quantile: Double,
        expectedLowerBound: Double? = null,
        expectedUpperBound: Double? = null
    ): WindowAggregateCheck<W> {
        this.constraints.add(ApproxQuantileConstraint(keyExpressionString, quantile, expectedLowerBound, expectedUpperBound))
        return this
    }
}
