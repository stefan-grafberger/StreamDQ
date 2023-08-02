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

import org.apache.datasketches.hll.TgtHllType
import org.apache.datasketches.hll.Union
import java.io.Serializable
import java.math.BigInteger
import org.apache.datasketches.hll.HllSketch as DataSketchHllSketch

class HllSketch : Serializable {
    var tgtHllType = TgtHllType.HLL_4
    private var lgk = 4
    var sketch: DataSketchHllSketch = DataSketchHllSketch(lgk)
    var totalCount: BigInteger = BigInteger.ZERO

    fun update(value: Any?) {
        this.totalCount += BigInteger.ONE
        when (value) {
            is Int -> { sketch.update(value.toLong()) }
            is Byte -> { sketch.update(byteArrayOf(value)) }
            is Char -> { sketch.update(charArrayOf(value)) }
            is Long -> { sketch.update(longArrayOf(value)) }
            is Double -> { sketch.update(value) }
            is LongArray -> { sketch.update(value) }
            is IntArray -> { sketch.update(value) }
            is CharArray -> { sketch.update(value) }
            is ByteArray -> { sketch.update(value) }
            is String -> { sketch.update(value) }
            null -> {}
            else -> {
                throw IllegalArgumentException("Type is not support currently: \"${value.javaClass.name}\"")
            }
        }
    }

    override fun toString(): String {
        return """HllSketchAggregation(${this.totalCount}, sketch=${this.sketch.estimate})"""
    }

    val result: Pair<BigInteger, Double>
        get() {
            return Pair(this.totalCount, this.sketch.estimate)
        }

    fun merge(edgeValue: HllSketch): HllSketch {
        val union = Union(lgk)
        union.update(edgeValue.sketch)
        union.update(this.sketch)
        this.sketch = union.result
        val newTotalCount = this.totalCount + edgeValue.totalCount
        this.totalCount = newTotalCount
        return this
    }
}
