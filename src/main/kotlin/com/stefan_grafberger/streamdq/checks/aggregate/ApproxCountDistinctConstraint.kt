package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import com.stefan_grafberger.streamdq.datasketches.HllSketchCountDistinctAggregate
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation

data class ApproxCountDistinctConstraint(
    override val keyExpressionString: String,
    val expectedLowerBound: Int? = null, // TODO: Non-Double Types
    val expectedUpperBound: Int? = null
) :
    AggregateConstraint() {

    override fun <T> getAggregateFunction(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableAggregateFunction<T> {
        return HllSketchCountDistinctAggregate(
            streamObjectTypeInfo, expectedLowerBound, expectedUpperBound,
            this.toString(), keyExpressionString, config
        )
    }
}
