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

package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SimpleThresholdStrategyTest {
    private lateinit var strategy: SimpleThresholdStrategy
    private val dataSeries = mutableListOf(-1.0, 2.0, 3.0, 0.5)

    @Test
    fun testDetectThresholdBoundIsNotOrderedExpectIllegalArgumentException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "The lower bound must be smaller or equal to the upper bound.",
                //when
                block = { strategy = SimpleThresholdStrategy(100.0, 10.0) }
        )
    }

    @Test
    fun testDetectWhenDataStreamComeExpectAnomalyStreamOutput() {
        //given
        val randomNum = Random(1)
        val newDataSeries = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }
        for (i in 20..30) {
            newDataSeries[i] = newDataSeries[i] + i + (i % 2 * -2 * i)
        }
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val expectedAnomalies = newDataSeries.filter { value -> value > 1.0 }.map { value -> AnomalyCheckResult(value, true) }
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second)
                .filter { result -> result.isAnomaly == true }
        val actualAnomalies = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalies,
                actualAnomalies)
    }
}
