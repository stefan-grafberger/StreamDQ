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

package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import java.util.Objects
import kotlin.reflect.KClass

data class ListLengthInRange(
    override val keyExpressionString: String,
    val expectedLowerBound: Int? = null,
    val expectedUpperBound: Int? = null
) :
    RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return ListLengthInRangeRowMapFunction(
            streamObjectTypeInfo, expectedLowerBound, expectedUpperBound,
            toString(), keyExpressionString, config
        )
    }
}

class ListLengthInRangeRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val expectedLowerBound: Int?,
    private val expectedUpperBound: Int?,
    val checkName: String,
    keyExpressionString: String,
    val config: ExecutionConfig?
) : TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any?>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }
    override fun map(value: T): RowLevelConstraintResult {
        val fieldValue = this.fieldAccessor.get(value)
        val listLength = if(fieldValue is Collection<*>) {
            fieldValue.size
        } else {
            throw NotImplementedError("Only Collections are supported for now")
        }

        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= listLength) &&
                (expectedUpperBound == null || expectedUpperBound >= listLength)
        return RowLevelConstraintResult(withinBounds, checkName)
    }
}
