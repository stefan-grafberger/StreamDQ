package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.accumulator.RelativeChangeAccumulator
import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * RelativeRateOfChangeAggregate function is used to detect anomalies
 * based on the data's relative change(division computation) in the stream.
 * Taking an sliding double-ended queue in the accumulator approach. The deque
 * has a length of order and stores the incoming element in the stream and It's
 * previously consecutively 2 elements
 *
 * This aggregate function is used in
 * [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.RelativeRateOfChangeStrategy]
 *
 * @param maxRateDecrease Upper bound of accepted decrease (lower bound of increase).
 * @param maxRateIncrease Upper bound of accepted growth.
 * @param order           order of the derivative
 */
class RelativeRateOfChangeAggregate(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1
) : AggregateFunction<AggregateConstraintResult,
        RelativeChangeAccumulator,
        AnomalyCheckResult> {

    private var currentValue = 0.0

    override fun createAccumulator(): RelativeChangeAccumulator {
        return RelativeChangeAccumulator(0.0, ArrayDeque(), 0.0, 0L)
    }

    override fun add(aggregateConstraintResult: AggregateConstraintResult, acc: RelativeChangeAccumulator): RelativeChangeAccumulator {
        currentValue = aggregateConstraintResult.aggregate!!
        if (acc.count < order) {
            acc.deque.add(currentValue)
        } else {
            val currentDenominator = acc.deque.removeFirst()
            acc.deque.add(currentValue)
            acc.currentChangeRate = currentValue / currentDenominator
        }
        acc.currentValue = currentValue
        acc.count += 1L
        return RelativeChangeAccumulator(acc.currentValue, acc.deque, acc.currentChangeRate, acc.count)
    }

    override fun getResult(acc: RelativeChangeAccumulator): AnomalyCheckResult {
        val currentRelativeChangeRate = acc.currentChangeRate
        if (acc.count > order && currentRelativeChangeRate !in maxRateDecrease..maxRateIncrease) {
            return AnomalyCheckResult(acc.currentValue, true)
        }
        return AnomalyCheckResult(acc.currentValue, false)
    }

    override fun merge(acc0: RelativeChangeAccumulator, acc1: RelativeChangeAccumulator): RelativeChangeAccumulator {
        return RelativeChangeAccumulator(acc0.currentValue + acc1.currentValue, ArrayDeque(acc0.deque.plus(acc1.deque)), acc0.currentChangeRate + acc1.currentChangeRate, acc0.count + acc1.count)
    }
}
