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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class KllSketchTest {
    @Test
    fun `test update`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1.toDouble())
        aggregation.update(4)
        aggregation.update(20)
        aggregation.update(80)
        aggregation.update(81.0)
        aggregation.update(85)
        aggregation.update(90.0)
        assertEquals(80.0, aggregation.result, 0.01)
    }

    @Test
    fun `test merge`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1.toDouble())
        aggregation.update(4)
        aggregation.update(20)
        aggregation.update(80)
        aggregation.update(81.0)

        val aggregation2 = KllSketch(0.5)
        aggregation.update(85)
        aggregation.update(90.0)

        val mergedAggregation = aggregation.merge(aggregation2)
        assertEquals(80.0, mergedAggregation.result, 0.01)
    }

    @Test
    fun `test updateSketch`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1)
        aggregation.update(2.toLong())
        aggregation.update(3.toByte())
        aggregation.update("10")
        aggregation.update("40")

        assertEquals(3.0, aggregation.result, 0.01)

        val exception = assertThrows(IllegalArgumentException::class.java) {
            aggregation.update(Object())
        }
        assertEquals("Type is not support currently: \"java.lang.Object\"", exception.message)
    }
}
