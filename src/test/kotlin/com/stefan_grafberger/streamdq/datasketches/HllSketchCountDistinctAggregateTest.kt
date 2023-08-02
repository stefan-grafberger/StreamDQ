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

import com.stefan_grafberger.streamdq.TestUtils.assertExpectedAggregates
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test

class HllSketchCountDistinctAggregateTest {

    @Test
    fun `test approx count distinct with window`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                HllSketchCountDistinctAggregate(
                    rawStream.type, null, null,
                    "test", "userId", env.config
                )
            )
        assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(3.0, 4.0, 1.0))

        val hllStreamTwo = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                HllSketchCountDistinctAggregate(
                    rawStream.type, null, null,
                    "test", "nestedInfo.nestedStringValue", env.config
                )
            )
        assertExpectedAggregates(hllStreamTwo, 3, doubleArrayOf(3.0, 5.0, 1.0))
    }

    @Test
    fun `test continuous approx count distinct with trigger`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                HllSketchCountDistinctAggregate(
                    rawStream.type, null, null,
                    "test", "userId", env.config
                )
            )
        assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(3.0, 4.0, 4.0))

        val hllTestStreamTwo = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                HllSketchCountDistinctAggregate(
                    rawStream.type, null, null,
                    "test", "nestedInfo.nestedStringValue", env.config
                )
            )
        assertExpectedAggregates(hllTestStreamTwo, 3, doubleArrayOf(1.0, 3.0, 5.0))
    }
}
