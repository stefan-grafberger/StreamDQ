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

import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory

class KllSketchAggregate<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val quantile: Double,
    val expectedLowerBound: Double? = null,
    val expectedUpperBound: Double? = null,
    private val checkName: String,
    val keyExpressionString: String,
    config: ExecutionConfig?
) :
    TypeQueryableAggregateFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun createAccumulator(): KllSketch {
        return KllSketch(this.quantile)
    }

    override fun add(input: T, kllSketchAggregation: Any): KllSketch {
        kllSketchAggregation as KllSketch
        val fieldValue = this.fieldAccessor.get(input)
        kllSketchAggregation.update(fieldValue)
        return kllSketchAggregation
    }

    override fun getResult(kllSketchAggregation: Any): AggregateConstraintResult {
        kllSketchAggregation as KllSketch
        val quantileValue = kllSketchAggregation.result
        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= quantileValue) &&
            (expectedUpperBound == null || expectedUpperBound >= quantileValue)
        // TODO: Float comparison and epsilon
        return AggregateConstraintResult(withinBounds, quantileValue, checkName)
    }

    override fun merge(hllSketchAggregation: Any, acc1: Any): KllSketch {
        hllSketchAggregation as KllSketch
        acc1 as KllSketch
        return acc1.merge(hllSketchAggregation)
    }

    override fun getIntermediateResultType(): TypeInformation<Any> {
        @Suppress("UNCHECKED_CAST")
        return TypeInformation.of(KllSketch::class.java) as TypeInformation<Any>
    }
}
