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

    fun withGlobalResultComputedFromResultsPerKey(
        computeGlobalResult: Boolean
    ): WindowAggregateCheck<W> {
        this.aggregateResultsPerKeyToGlobalResult = computeGlobalResult
        return this
    }
}
