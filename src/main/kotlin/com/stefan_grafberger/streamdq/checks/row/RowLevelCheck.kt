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

import com.stefan_grafberger.streamdq.checks.Check
import com.stefan_grafberger.streamdq.checks.Constraint
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import java.math.BigDecimal
import java.util.regex.Pattern

data class RowLevelCheck(val constraints: MutableList<RowLevelConstraint> = mutableListOf()) : Check() {

    fun isComplete(keyExpressionString: String): RowLevelCheck {
        this.constraints.add(RowValueComplete(keyExpressionString))
        return this
    }

    fun isContainedIn(keyExpressionString: String, expectedValues: Collection<Any>): RowLevelCheck {
        this.constraints.add(RowValueContainedIn(keyExpressionString, expectedValues))
        return this
    }

    fun isInRange(
        keyExpressionString: String,
        expectedLowerBound: BigDecimal? = null,
        expectedUpperBound: BigDecimal? = null
    ): RowLevelCheck {
        this.constraints.add(RowValueInRange(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }

    fun matchesPattern(
        keyExpressionString: String,
        regexExpression: Pattern
    ): RowLevelCheck {
        this.constraints.add(RowValueMatchesPattern(keyExpressionString, regexExpression))
        return this
    }

    fun isNonNegative(keyExpressionString: String): RowLevelCheck {
        this.constraints.add(RowValueIsNonNegative(keyExpressionString))
        return this
    }

    fun listLengthInRange(
        keyExpressionString: String,
        expectedLowerBound: Int? = null,
        expectedUpperBound: Int? = null
    ): RowLevelCheck {
        this.constraints.add(ListLengthInRange(keyExpressionString, expectedLowerBound, expectedUpperBound))
        return this
    }
}

abstract class RowLevelConstraint : Constraint() {
    abstract val keyExpressionString: String
    abstract fun <T> getCheckResultMapper(streamObjectTypeInfo: TypeInformation<T>, config: ExecutionConfig?): TypeQueryableRowMapFunction<T>
}
