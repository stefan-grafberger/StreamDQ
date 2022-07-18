package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import java.math.BigDecimal

data class RowValueInRange(
    override val keyExpressionString: String,
    val expectedLowerBound: BigDecimal? = null, // TODO: Non-Double Types
    val expectedUpperBound: BigDecimal? = null
) :
    RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return RowValueInRangeRowMapFunction(
            streamObjectTypeInfo, expectedLowerBound, expectedUpperBound,
            toString(), keyExpressionString, config
        )
    }
}

class RowValueInRangeRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val expectedLowerBound: BigDecimal?,
    private val expectedUpperBound: BigDecimal?,
    val checkName: String,
    keyExpressionString: String,
    val config: ExecutionConfig?
) : TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun map(value: T): RowLevelConstraintResult {
        // TODO: Support different comparable types
        val fieldValue = this.fieldAccessor.get(value)
        val bidDecimalFieldValue = if (fieldValue is Long) {
            BigDecimal.valueOf(fieldValue)
        } else if (fieldValue is Double) {
            BigDecimal.valueOf(fieldValue)
        } else {
            throw NotImplementedError("TODO: Support more comparable types")
        }

        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= bidDecimalFieldValue) &&
            (expectedUpperBound == null || expectedUpperBound >= bidDecimalFieldValue)
        return RowLevelConstraintResult(withinBounds, checkName)
    }
}
