package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.accumulator.NormalStrategyAccumulator
import com.stefan_grafberger.streamdq.anomalydetection.model.result.NormalStrategyResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import kotlin.math.sqrt

/**
 * Online normal aggregate function for detecting anomaly.
 * detecting anomalies based on a customized bound on the
 * number of standard deviations they are allowed to be
 * different from the mean
 *
 * This aggregate function is used in
 * [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy]
 *
 * @see <a href="https://fanlastVariance.user.srcf.net/hermes/doc/antiforgery/stats.pdf">Incremental calculation of variance</a>
 */
class OnlineNormalAggregate(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
) : AggregateFunction<AggregateConstraintResult,
        NormalStrategyAccumulator,
        NormalStrategyResult> {

    private var currentValue = 0.0

    /**
     * accumulator used to preserve current mean of aggregate values,
     * current variance and count of the elements
     */
    override fun createAccumulator(): NormalStrategyAccumulator {
        return NormalStrategyAccumulator(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L)
    }

    override fun add(
            aggregateConstraintResult: AggregateConstraintResult,
            acc: NormalStrategyAccumulator
    )
            : NormalStrategyAccumulator {

        currentValue = aggregateConstraintResult.aggregate!!

        acc.lastMean = acc.currentMean
        acc.lastVariance = acc.currentVariance
        acc.lastSn = acc.sn

        acc.currentMean = if (acc.count == 0L) {
            currentValue
        } else {
            acc.lastMean + (1.0 / (acc.count + 1)) * (currentValue - acc.lastMean)
        }

        acc.sn += (currentValue - acc.lastMean) * (currentValue - acc.currentMean)
        acc.currentVariance = acc.sn / (acc.count + 1)

        return NormalStrategyAccumulator(
                acc.lastMean, acc.currentMean, acc.lastVariance, acc.currentVariance,
                acc.lastSn, acc.sn, acc.count + 1L
        )
    }

    override fun getResult(acc: NormalStrategyAccumulator): NormalStrategyResult {
        val stdDev = sqrt(acc.currentVariance)
        val upperBound = acc.currentMean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = acc.currentMean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        return if (currentValue in lowerBound..upperBound) {
            NormalStrategyResult(currentValue, acc.currentMean, stdDev, isAnomaly = false)
        } else {
            // AnomalyCheckResult won't affect mean and variance
            acc.currentMean = acc.lastMean
            acc.currentVariance = acc.lastVariance
            acc.sn = acc.lastSn
            NormalStrategyResult(currentValue, acc.currentMean, stdDev, isAnomaly = true)
        }
    }

    override fun merge(
            acc0: NormalStrategyAccumulator,
            acc1: NormalStrategyAccumulator
    )
            : NormalStrategyAccumulator {
        return NormalStrategyAccumulator(
                acc1.lastMean, acc1.currentMean, acc1.lastVariance, acc1.currentVariance,
                acc1.lastSn, acc1.sn, acc1.count + 1L
        )
    }
}