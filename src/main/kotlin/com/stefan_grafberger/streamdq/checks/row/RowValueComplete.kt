package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory

data class RowValueComplete(override val keyExpressionString: String) : RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return RowValueCompleteRowMapFunction(
            streamObjectTypeInfo, toString(), // TODO: Remove valueIndex
            keyExpressionString, config
        )
    }
}

class RowValueCompleteRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val checkName: String,
    val keyExpressionString: String,
    val config: ExecutionConfig?
) :
    TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun map(value: T): RowLevelConstraintResult {
        // TODO: Support different comparable types
        val fieldValue = this.fieldAccessor.get(value)
        val result = fieldValue != null
        return RowLevelConstraintResult(result, checkName)
    }
}
