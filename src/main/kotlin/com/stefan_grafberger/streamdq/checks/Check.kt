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
    override var constraintName: String? = null
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
