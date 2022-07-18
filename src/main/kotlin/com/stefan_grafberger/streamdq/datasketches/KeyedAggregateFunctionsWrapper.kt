package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row

class KeyedAggregateFunctionsWrapper<T>(val aggregateFunctions: List<TypeQueryableAggregateFunction<T>>) :
    AggregateFunction<T, Any, Row>, ResultTypeQueryable<Row> {

    override fun createAccumulator(): Row {
        val accumulators = aggregateFunctions.map { aggregateFunction -> aggregateFunction.createAccumulator() }
            .toTypedArray()
        return Row.of(*accumulators)
    }

    override fun add(input: T, rowAggregation: Any): Row {
        rowAggregation as Row
        val additionResults = aggregateFunctions
            .zip(0 until rowAggregation.arity)
            .map { (aggregateFunction, aggregationIndex) ->
                val currentAggregate = rowAggregation.getField(aggregationIndex)
                aggregateFunction.add(input, currentAggregate)
            }.toTypedArray()
        return Row.of(*additionResults)
    }

    override fun getResult(rowAggregation: Any): Row {
        return rowAggregation as Row
    }

    override fun merge(rowAggregation: Any, rowAcc1: Any): Row {
        rowAggregation as Row
        rowAcc1 as Row
        val aggregationResults = aggregateFunctions
            .zip(0 until rowAggregation.arity)
            .map { (aggregateFunction, aggregationIndex) ->
                val currentAggregate = rowAggregation.getField(aggregationIndex)
                val currentAcc1 = rowAcc1.getField(aggregationIndex)
                aggregateFunction.merge(currentAggregate, currentAcc1)
            }.toTypedArray()
        return Row.of(*aggregationResults)
    }

    override fun getProducedType(): RowTypeInfo {
        val wrappedTypes = aggregateFunctions.map { aggregateFunction -> aggregateFunction.getIntermediateResultType() }
            .toTypedArray()
        return RowTypeInfo(*wrappedTypes)
    }
}
