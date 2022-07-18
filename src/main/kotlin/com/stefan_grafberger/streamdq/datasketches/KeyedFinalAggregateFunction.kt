package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row

class KeyedFinalAggregateFunction<IN>(val aggregateFunctions: List<TypeQueryableAggregateFunction<IN>>) :
    AggregateFunction<Row, Any, AggregateCheckResult>, ResultTypeQueryable<AggregateCheckResult> {

    override fun createAccumulator(): Row {
        val accumulators = aggregateFunctions.map { aggregateFunction -> aggregateFunction.createAccumulator() }
            .toTypedArray()
        return Row.of(*accumulators)
    }

    override fun add(input: Row, rowAggregation: Any): Row {
        rowAggregation as Row
        return this.merge(rowAggregation, input)
    }

    override fun getResult(rowAggregation: Any): AggregateCheckResult {
        rowAggregation as Row
        val aggregationResults = aggregateFunctions
            .zip(0 until rowAggregation.arity)
            .map { (aggregateFunction, aggregationIndex) ->
                val currentAggregate = rowAggregation.getField(aggregationIndex)
                aggregateFunction.getResult(currentAggregate)
            }.toList()
        return AggregateCheckResult(aggregationResults as List<AggregateConstraintResult>)
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

    override fun getProducedType(): TypeInformation<AggregateCheckResult> {
        return TypeInformation.of(AggregateCheckResult::class.java)
    }
}
