/**
 * Licensed to the University of Amsterdam (UvA) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The UvA licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

class KeyedFinalAggregateFunction<IN, KEY>(val aggregateFunctions: List<TypeQueryableAggregateFunction<IN>>) :
    AggregateFunction<Row, Any, AggregateCheckResult<Any>>, ResultTypeQueryable<AggregateCheckResult<KEY>> {

    override fun createAccumulator(): Row {
        val accumulators = aggregateFunctions.map { aggregateFunction -> aggregateFunction.createAccumulator() }
            .toTypedArray()
        return Row.of(*accumulators)
    }

    override fun add(input: Row, rowAggregation: Any): Row {
        rowAggregation as Row
        return this.merge(rowAggregation, input)
    }

    override fun getResult(rowAggregation: Any): AggregateCheckResult<Any> {
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

    override fun getProducedType(): TypeInformation<AggregateCheckResult<KEY>> {
        @Suppress("UNCHECKED_CAST")
        return TypeInformation.of(AggregateCheckResult::class.java) as TypeInformation<AggregateCheckResult<KEY>>
    }
}

class AddKeyInfo<KEY, WINDOW : Window> :
    ProcessWindowFunction<AggregateCheckResult<KEY>?, AggregateCheckResult<KEY>?, KEY?, WINDOW?>() {
    override fun process(
        key: KEY?,
        context: Context?,
        aggregationResults: MutableIterable<AggregateCheckResult<KEY>?>?,
        out: Collector<AggregateCheckResult<KEY>?>?
    ) {
        val outputList = aggregationResults?.iterator()?.asSequence()?.toList()
        outputList?.forEach { aggregationResult ->
            aggregationResult?.partitionKeyValue = key
            out?.collect(aggregationResult)
        }
    }
}
