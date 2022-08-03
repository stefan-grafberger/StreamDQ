package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.checks.Check
import com.stefan_grafberger.streamdq.checks.Constraint
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.AllWindowedStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window

abstract class InternalAggregateCheck : Check() {
    val constraints: MutableList<AggregateConstraint> = mutableListOf()
    var aggregateResultsPerKeyToGlobalResult: Boolean = true
    abstract fun <IN, KEY> addWindowOrTriggerKeyed(accessedfieldStream: KeyedStream<IN, KEY>):
        WindowedStream<IN, KEY, Window>
    abstract fun <IN> addWindowOrTriggerNonKeyed(accessedfieldStream: DataStream<IN>, mergeKeyedResultsOnly: Boolean):
        AllWindowedStream<IN, Window>
}

class AggregateCheck {
    fun <W : Window> onContinuousStreamWithTrigger(trigger: Trigger<Any?, W>): ContinuousAggregateCheck<W> {
        return ContinuousAggregateCheck(trigger)
    }

    fun <W : Window> onWindow(aggregateWindowAssigner: WindowAssigner<Any?, W>): WindowAggregateCheck<W> {
        return WindowAggregateCheck(aggregateWindowAssigner)
    }
}

abstract class AggregateConstraint : Constraint() {
    abstract val keyExpressionString: String
    abstract fun <T> getAggregateFunction(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableAggregateFunction<T>
}
