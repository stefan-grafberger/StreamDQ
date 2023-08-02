/**
 * Licensed to the University of Amsterdam (UvA) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The UvA licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
