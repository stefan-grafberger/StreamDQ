package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory

data class RowValueContainedIn(override val keyExpressionString: String, val expectedValues: Collection<Any>) :
    RowLevelConstraint() {
    override fun <T> getCheckResultMapper(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableRowMapFunction<T> {
        return RowValueContainedInRowMapFunction(
            streamObjectTypeInfo, expectedValues.toHashSet(), toString(),
            keyExpressionString, config
        )
    }
}

class RowValueContainedInRowMapFunction<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val expectedValues: Set<Any>,
    private val checkName: String,
    keyExpressionString: String,
    val config: ExecutionConfig?
) :
    TypeQueryableRowMapFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun map(value: T): RowLevelConstraintResult {
        val fieldValue = this.fieldAccessor.get(value)
        val result = fieldValue in expectedValues
        return RowLevelConstraintResult(result, checkName)
    }
}
