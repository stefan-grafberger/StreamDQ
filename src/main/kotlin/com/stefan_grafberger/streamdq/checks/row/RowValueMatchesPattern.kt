package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import java.util.regex.Pattern

data class RowValueMatchesPattern(
    override val keyExpressionString: String,
    val regexExpression: Pattern
) :
    RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return RowValueMatchesRegexRowMapFunction(
            streamObjectTypeInfo, regexExpression, toString(),
            keyExpressionString, config
        )
    }
}

class RowValueMatchesRegexRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val regexExpression: Pattern,
    private val checkName: String,
    keyExpressionString: String,
    val config: ExecutionConfig?
) : TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun map(value: T): RowLevelConstraintResult {
        // TODO: Support different comparable types
        val fieldValue = this.fieldAccessor.get(value) as String
        val regexMatch = regexExpression.matcher(fieldValue).matches()
        return RowLevelConstraintResult(regexMatch, checkName)
    }
}
