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

import org.apache.datasketches.kll.KllFloatsSketch
import java.io.Serializable
import kotlin.math.abs

class KllSketch(private val quantile: Double) : Serializable {
    var sketch: KllFloatsSketch = KllFloatsSketch.newHeapInstance()

    fun update(value: Any?) {
        when (value) {
            is Int -> { sketch.update(value.toFloat()) }
            is Byte -> { sketch.update(value.toFloat()) }
            is Char -> { sketch.update(value.toFloat()) }
            is Long -> { sketch.update(value.toFloat()) }
            is Double -> { sketch.update(value.toFloat()) }
            is String -> { sketch.update(value.toFloat()) }
            null -> {}
            else -> {
                throw IllegalArgumentException("Type is not support currently: \"${value.javaClass.name}\"")
            }
        }
    }

    override fun toString(): String {
        return """KllSketchAggregation(sketch=${this.sketch})"""
    }

    val result: Double
        get() {
            return this.sketch.getQuantile(this.quantile).toDouble()
        }

    fun merge(edgeValue: KllSketch): KllSketch {
        if (abs(this.quantile / edgeValue.quantile - 1) >= 0.000001) {
            throw IllegalArgumentException(
                "Both KllSketches must be for the same quantile! " +
                    "This sketch has quantile '${this.quantile}', the other has '${edgeValue.quantile}'!"
            )
        }
        this.sketch.merge(edgeValue.sketch)
        return this
    }
}
