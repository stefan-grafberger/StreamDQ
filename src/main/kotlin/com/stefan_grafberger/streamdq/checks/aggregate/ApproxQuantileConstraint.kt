package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import com.stefan_grafberger.streamdq.datasketches.KllSketchAggregate
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation

data class ApproxQuantileConstraint(
    override val keyExpressionString: String,
    val quantile: Double,
    val expectedLowerBound: Double? = null, // TODO: Non-Double Types
    val expectedUpperBound: Double? = null
) :
    AggregateConstraint() {

    override fun <T> getAggregateFunction(
        streamObjectTypeInfo: TypeInformation<T>,
        config: ExecutionConfig?
    ): TypeQueryableAggregateFunction<T> {
        return KllSketchAggregate(
            streamObjectTypeInfo, quantile, expectedLowerBound, expectedUpperBound,
            this.toString(), keyExpressionString, config
        )
    }
}
