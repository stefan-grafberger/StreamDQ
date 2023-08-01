package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.dto.NormalStrategyResultDto
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple7
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
 * @see <a href="https://fanf2.user.srcf.net/hermes/doc/antiforgery/stats.pdf">Incremental calculation of variance</a>
 */
class OnlineNormalAggregate(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
) : AggregateFunction<AggregateConstraintResult,
        Tuple7<Double, Double, Double, Double, Double, Double, Long>,
        NormalStrategyResultDto> {

    private var currentValue = 0.0

    /**
     * accumulator used to preserve current mean of aggregate values,
     * current variance and count of the elements
     * Tuple7
     * (lastMean f0, currentMean f1, lastVariance f2,
     * currentVariance f3, lastSn f4, sn f5, count f6)
     */
    override fun createAccumulator(): Tuple7<Double, Double, Double, Double, Double, Double, Long> {
        return Tuple7(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L)
    }

    override fun add(
            aggregateConstraintResult: AggregateConstraintResult,
            acc: Tuple7<Double, Double, Double, Double, Double, Double, Long>
    )
            : Tuple7<Double, Double, Double, Double, Double, Double, Long> {

        currentValue = aggregateConstraintResult.aggregate!!

        acc.f0 = acc.f1
        acc.f2 = acc.f3
        acc.f4 = acc.f5

        acc.f1 = if (acc.f6 == 0L) {
            currentValue
        } else {
            acc.f0 + (1.0 / (acc.f6 + 1)) * (currentValue - acc.f0)
        }

        acc.f5 += (currentValue - acc.f0) * (currentValue - acc.f1)
        acc.f3 = acc.f5 / (acc.f6 + 1)

        return Tuple7<Double, Double, Double, Double, Double, Double, Long>(
                acc.f0, acc.f1, acc.f2, acc.f3,
                acc.f4, acc.f5, acc.f6 + 1L
        )
    }

    override fun getResult(acc: Tuple7<Double, Double, Double, Double, Double, Double, Long>): NormalStrategyResultDto {
        val stdDev = sqrt(acc.f3)
        val upperBound = acc.f1 + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = acc.f1 - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        return if (currentValue in lowerBound..upperBound) {
            NormalStrategyResultDto(currentValue, acc.f1, stdDev, isAnomaly = false)
        } else {
            // AnomalyCheckResult won't affect mean and variance
            acc.f1 = acc.f0
            acc.f3 = acc.f2
            acc.f5 = acc.f4
            NormalStrategyResultDto(currentValue, acc.f1, stdDev, isAnomaly = true)
        }
    }

    override fun merge(
            acc0: Tuple7<Double, Double, Double, Double, Double, Double, Long>,
            acc1: Tuple7<Double, Double, Double, Double, Double, Double, Long>
    )
            : Tuple7<Double, Double, Double, Double, Double, Double, Long> {
        return Tuple7<Double, Double, Double, Double, Double, Double, Long>(
                acc1.f0, acc1.f1, acc1.f2, acc1.f3,
                acc1.f4, acc1.f5, acc1.f6 + 1L
        )
    }
}