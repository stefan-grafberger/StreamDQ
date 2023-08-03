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
