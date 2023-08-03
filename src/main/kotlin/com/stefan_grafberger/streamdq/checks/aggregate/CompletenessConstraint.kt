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

import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import java.math.BigDecimal
import java.math.BigInteger

data class CompletenessConstraint(
    override val keyExpressionString: String,
    val expectedLowerBound: Double? = null, // TODO: Non-Double Types
    val expectedUpperBound: Double? = null
) :
    AggregateConstraint() {

    override fun <T> getAggregateFunction(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableAggregateFunction<T> {
        return CompletenessWindowAggregate(
            streamObjectTypeInfo, expectedLowerBound, expectedUpperBound,
            this.toString(), keyExpressionString, config
        )
    }
}

class CompletenessWindowAggregate<T>(
    streamObjectTypeInfo: TypeInformation<T>,
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

    override fun createAccumulator(): Tuple2<BigInteger, BigInteger> {
        return Tuple2(BigInteger.ZERO, BigInteger.ZERO)
    }

    override fun add(input: T, completenessAggregate: Any): Tuple2<BigInteger, BigInteger> {
        @Suppress("UNCHECKED_CAST")
        completenessAggregate as Tuple2<BigInteger, BigInteger>
        val totalCount = completenessAggregate.f0
        val compliantCount = completenessAggregate.f1
        val fieldValue = this.fieldAccessor.get(input)
        val isNotNull = (fieldValue != null)
        val completenessAdd = if (isNotNull) BigInteger.ONE else BigInteger.ZERO
        val newTotalCount = totalCount + BigInteger.ONE
        val newCompliantCount = compliantCount + completenessAdd
        return Tuple2(newTotalCount, newCompliantCount)
    }

    override fun getResult(completenessAggregate: Any): AggregateConstraintResult {
        @Suppress("UNCHECKED_CAST")
        completenessAggregate as Tuple2<BigInteger, BigInteger>
        val totalCount = BigDecimal(completenessAggregate.f0)
        val compliantCount = BigDecimal(completenessAggregate.f1)
        val compliance = compliantCount.divide(
            totalCount, PrecisionConstants.BIG_DECIMAL_SCALE, PrecisionConstants.ROUNDING_MODE
        ).toDouble()
        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= compliance) &&
            (expectedUpperBound == null || expectedUpperBound >= compliance)
        return AggregateConstraintResult(withinBounds, compliance, checkName)
    }

    override fun merge(completenessAggregate: Any, acc1: Any): Tuple2<BigInteger, BigInteger> {
        @Suppress("UNCHECKED_CAST")
        completenessAggregate as Tuple2<BigInteger, BigInteger>
        @Suppress("UNCHECKED_CAST")
        acc1 as Tuple2<BigInteger, BigInteger>
        return Tuple2(completenessAggregate.f0 + acc1.f0, completenessAggregate.f1 + acc1.f1)
    }

    override fun getIntermediateResultType(): TypeInformation<Any> {
        @Suppress("UNCHECKED_CAST")
        return TypeInformation.of(object : TypeHint<Tuple2<BigInteger, BigInteger>>() {}) as TypeInformation<Any>
    }
}
