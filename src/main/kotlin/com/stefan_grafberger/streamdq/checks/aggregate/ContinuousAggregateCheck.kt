package com.stefan_grafberger.streamdq.checks.aggregate

import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.windows.Window

data class ContinuousAggregateCheck<W : Window> (val trigger: Trigger<Any?, W>) : InternalAggregateCheck() {
    override fun <IN, KEY> addWindowOrTriggerKeyed(accessedfieldStream: KeyedStream<IN, KEY>): WindowedStream<IN, KEY, Window> {
        val windowedStream = accessedfieldStream.window(GlobalWindows.create())
        @Suppress("UNCHECKED_CAST")
        this.trigger as Trigger<Any?, Window>
        val triggerStream = windowedStream.trigger(this.trigger)
        @Suppress("UNCHECKED_CAST")
        return triggerStream as WindowedStream<IN, KEY, Window>
    }

    override fun <IN> addWindowOrTriggerNonKeyed(
        accessedfieldStream: DataStream<IN>,
        mergeKeyedResultsOnly: Boolean
    ): AllWindowedStream<IN, Window> {
        val windowedStream = accessedfieldStream.windowAll(GlobalWindows.create())
        val triggerStream = if (this.trigger is CountTrigger && mergeKeyedResultsOnly) {
            // TODO: When the window/trigger is not time-based, the order can get messed up
            //  when stream is partitioned and everything is count based only.
            //  In that case, we could also think about using countTriggerAll and process everything
            //  without partitioning to guarantee correct results. However, performance is horrible then.
            //  With this approach here, the result can be computed efficiently if reordering is okay.
            val newTrigger = CountTrigger.of<GlobalWindow>(1)
            windowedStream.trigger(newTrigger)
        } else {
            @Suppress("UNCHECKED_CAST")
            this.trigger as Trigger<Any?, Window>
            windowedStream.trigger(this.trigger)
        }
        @Suppress("UNCHECKED_CAST")
        return triggerStream as AllWindowedStream<IN, Window>
    }

    fun hasCompletenessBetween(
        keyExpressionString: String,
        expectedLowerBound: Double? = null,
        expectedUpperBound: Double? = null
    ): ContinuousAggregateCheck<W> {
        this.constraints.add(CompletenessConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxCountDistinctBetween(
        keyExpressionString: String,
        expectedLowerBound: Int? = null,
        expectedUpperBound: Int? = null
    ): ContinuousAggregateCheck<W> {
        this.constraints.add(ApproxCountDistinctConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxUniquenessBetween(
        keyExpressionString: String,
        expectedLowerBound: Double? = null, // TODO: Non-Double Types
        expectedUpperBound: Double? = null
    ): ContinuousAggregateCheck<W> {
        this.constraints.add(ApproxUniquenessConstraint(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun hasApproxQuantileBetween(
        keyExpressionString: String,
        quantile: Double,
        expectedLowerBound: Double? = null,
        expectedUpperBound: Double? = null
    ): ContinuousAggregateCheck<W> {
        this.constraints.add(ApproxQuantileConstraint(keyExpressionString, quantile, expectedLowerBound, expectedUpperBound))
        return this
    }
}
