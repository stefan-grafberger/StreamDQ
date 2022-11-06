package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import java.math.BigDecimal

data class RowValueIsNonNegative(
    override val keyExpressionString: String
):
    RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return RowValueIsNonNegativeRowMapFunction(
            streamObjectTypeInfo,
            toString(),
            keyExpressionString, config
        )
    }
}

class RowValueIsNonNegativeRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    val checkName: String,
    val keyExpressionString: String,
    val config: ExecutionConfig?
) : TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun map(value: T): RowLevelConstraintResult {
        // TODO: Support different comparable types
        val fieldValue = this.fieldAccessor.get(value)
        val bigDecimalFieldValue = if (fieldValue is Long) {
            BigDecimal.valueOf(fieldValue)
        } else if (fieldValue is Double) {
            BigDecimal.valueOf(fieldValue)
        } else {
            throw NotImplementedError("TODO: Support more comparable types")
        }
        val notLessThan: (BigDecimal, BigDecimal) -> Boolean = { x: BigDecimal, y: BigDecimal -> x >= y }
        val isNonNegative = notLessThan(bigDecimalFieldValue,BigDecimal(0))
        return RowLevelConstraintResult(isNonNegative, checkName)
    }
}
