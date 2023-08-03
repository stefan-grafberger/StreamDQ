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

abstract class Check {
    abstract override fun equals(other: Any?): Boolean
    abstract override fun hashCode(): Int
}

abstract class Constraint {
    abstract override fun equals(other: Any?): Boolean
    abstract override fun hashCode(): Int
}

abstract class ConstraintResult {
    abstract var outcome: Boolean?
    abstract var constraintName: String?
}

abstract class CheckResult<T : ConstraintResult> {
    abstract var constraintResults: List<T>? // TODO: Map?
}

// properties are `var`s with default `null` instead of `val`s, so that Flink serialization can treat it as a
// POJO with proper setters

data class AggregateConstraintResult(
    override var outcome: Boolean? = null,
    var aggregate: Double? = null,
    override var constraintName: String? = null,
    var timestamp: Long = 0
) : ConstraintResult()

data class AggregateCheckResult<KEY>(
    override var constraintResults: List<AggregateConstraintResult>? = null,
    var partitionKeyValue: KEY? = null
) : CheckResult<AggregateConstraintResult>()

data class RowLevelConstraintResult(
    override var outcome: Boolean? = null,
    override var constraintName: String? = null
) : ConstraintResult()

data class RowLevelCheckResult<IN>(override var constraintResults: List<RowLevelConstraintResult>? = null, var checkedObject: IN? = null) : CheckResult<RowLevelConstraintResult>()
