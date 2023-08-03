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

import com.stefan_grafberger.streamdq.TestUtils.assertAggregateConstraintResultsWithNameAsString
import com.stefan_grafberger.streamdq.data.ClickInfo
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class NonKeyedAggregateFunctionsWrapperTest {

    @Test
    fun `test wrapper with window`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val aggregateFunctions = listOf(
            KllSketchAggregate(rawStream.type, 0.5, null, null, "test", "intValue", env.config),
            KllSketchAggregate(rawStream.type, 0.9, null, null, "test", "nestedInfo.nestedDoubleValue", env.config)
        )

        val functionWrapper = NonKeyedAggregateFunctionsWrapper<ClickInfo, Any>(aggregateFunctions)
        val aggregateFunctionResults = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(functionWrapper)

        val collectedResultOne = aggregateFunctionResults!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(3, collectedResultOne.size)

        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 0, "test",
            doubleArrayOf(10.0, 8.0, 1.0), arrayOf(true, true, true)
        )
        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 1, "test",
            doubleArrayOf(8.6, 15.1, 4.19), arrayOf(true, true, true)
        )
    }

    @Test
    fun `test wrapper with continuous`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val aggregateFunctions = listOf(
            KllSketchAggregate(rawStream.type, 0.5, null, null, "test", "intValue", env.config),
            KllSketchAggregate(rawStream.type, 0.9, null, null, "test", "nestedInfo.nestedDoubleValue", env.config)
        )

        val functionWrapper = NonKeyedAggregateFunctionsWrapper<ClickInfo, Any>(aggregateFunctions)
        val aggregateFunctionResults = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(functionWrapper)

        val collectedResultOne = aggregateFunctionResults!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(3, collectedResultOne.size)

        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 0, "test",
            doubleArrayOf(5.0, 10.0, 8.0), arrayOf(true, true, true)
        )
        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 1, "test",
            doubleArrayOf(6.09, 9.6, 15.1), arrayOf(true, true, true)
        )
    }
}
