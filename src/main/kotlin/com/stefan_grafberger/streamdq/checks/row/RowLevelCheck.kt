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
}

abstract class RowLevelConstraint : Constraint() {
    abstract val keyExpressionString: String
    abstract fun <T> getCheckResultMapper(streamObjectTypeInfo: TypeInformation<T>, config: ExecutionConfig?): TypeQueryableRowMapFunction<T>
}
