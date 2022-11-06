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
        // TODO: Support different comparable types
        val fieldValue = this.fieldAccessor.get(value)
        val listLength = if(fieldValue is List<*>) {
            fieldValue.size
        } else {
            throw NotImplementedError("TODO: Support more comparable types")
        }

        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= listLength) &&
                (expectedUpperBound == null || expectedUpperBound >= listLength)
        return RowLevelConstraintResult(withinBounds, checkName)
    }
}
