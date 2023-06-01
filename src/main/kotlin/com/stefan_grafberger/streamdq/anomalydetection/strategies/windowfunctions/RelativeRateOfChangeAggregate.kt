package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple4

class RelativeRateOfChangeAggregate(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1
) : AggregateFunction<AggregateConstraintResult,
        Tuple4<Double, ArrayDeque<Double>, Double, Long>,
        AnomalyCheckResult> {

    private var currentValue = 0.0

    override fun createAccumulator(): Tuple4<Double, ArrayDeque<Double>, Double, Long> {
        return Tuple4(0.0, ArrayDeque(), 0.0, 0L)
    }

    override fun add(aggregateConstraintResult: AggregateConstraintResult, acc: Tuple4<Double, ArrayDeque<Double>, Double, Long>): Tuple4<Double, ArrayDeque<Double>, Double, Long> {
        currentValue = aggregateConstraintResult.aggregate!!
        if (acc.f3 < order) {
            acc.f1.add(currentValue)
        } else {
            val currentDenominator = acc.f1.removeFirst()
            acc.f1.add(currentValue)
            acc.f2 = currentValue / currentDenominator
        }
        acc.f0 = currentValue
        acc.f3 += 1L
        return Tuple4(acc.f0, acc.f1, acc.f2, acc.f3)
    }

    override fun getResult(acc: Tuple4<Double, ArrayDeque<Double>, Double, Long>): AnomalyCheckResult {
        val currentRelativeChangeRate = acc.f2
        if (acc.f3 > order && currentRelativeChangeRate !in maxRateDecrease..maxRateIncrease) {
            return AnomalyCheckResult(acc.f0, true, 1.0)
        }
        return AnomalyCheckResult(acc.f0, false, 1.0)
    }

    override fun merge(acc0: Tuple4<Double, ArrayDeque<Double>, Double, Long>, acc1: Tuple4<Double, ArrayDeque<Double>, Double, Long>): Tuple4<Double, ArrayDeque<Double>, Double, Long> {
        return Tuple4(acc0.f0 + acc1.f0, ArrayDeque(acc0.f1.plus(acc1.f1)), acc0.f2 + acc1.f2, acc0.f3 + acc1.f3)
    }
}
