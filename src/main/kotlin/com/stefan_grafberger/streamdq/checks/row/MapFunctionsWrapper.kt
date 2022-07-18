package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableRowMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable

class MapFunctionsWrapper<T>(private val mapFunctions: List<TypeQueryableRowMapFunction<T>>) :
    MapFunction<T, RowLevelCheckResult<T>>, ResultTypeQueryable<RowLevelCheckResult<T>> {

    override fun getProducedType(): TypeInformation<RowLevelCheckResult<T>> {
        return TypeInformation.of(object : TypeHint<RowLevelCheckResult<T>>() {})
    }

    override fun map(value: T): RowLevelCheckResult<T> {
        val mapResults = mapFunctions
            .map { mapFunction ->
                mapFunction.map(value)
            }.toList()
        return RowLevelCheckResult(mapResults as List<RowLevelConstraintResult>, value)
    }
}
