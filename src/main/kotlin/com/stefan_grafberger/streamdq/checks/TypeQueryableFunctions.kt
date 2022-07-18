package com.stefan_grafberger.streamdq.checks

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable

abstract class TypeQueryableRowMapFunction<T> :
    MapFunction<T, RowLevelConstraintResult>,
    ResultTypeQueryable<RowLevelConstraintResult> {
    override fun getProducedType(): TypeInformation<RowLevelConstraintResult> {
        return TypeInformation.of(RowLevelConstraintResult::class.java)
    }
}

abstract class TypeQueryableAggregateFunction<T> :
    AggregateFunction<T, Any, AggregateConstraintResult>,
    ResultTypeQueryable<AggregateConstraintResult> {
    override fun getProducedType(): TypeInformation<AggregateConstraintResult> {
        return TypeInformation.of(AggregateConstraintResult::class.java)
    }

    open fun getIntermediateResultType(): TypeInformation<Any>? {
        return null
    }
}
