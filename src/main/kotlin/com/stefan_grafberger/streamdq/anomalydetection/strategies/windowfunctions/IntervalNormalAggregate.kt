package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.NormalStrategyResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple3
import kotlin.math.pow
import kotlin.math.sqrt

@Deprecated("Use OnlineNormalAggregate with sliding window replace this")
class IntervalNormalAggregate(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
        private val includeInterval: Boolean = false,
        private val waterMarkInterval: Pair<Long, Long>?)
    : AggregateFunction<AggregateConstraintResult, Tuple3<Double, Double, Long>, NormalStrategyResult> {

    private var currentValue = 0.0
    private var currentTimeStamp = 0L

    /**
     * accumulator used to preserve current mean of aggregate values,
     * current variance and count of the elements
     * Tuple3
     * (currentMean f0, currentVariance f1, count f2)
     */
    override fun createAccumulator(): Tuple3<Double, Double, Long> {
        return Tuple3<Double, Double, Long>(0.0, 0.0, 0L)
    }

    override fun add(aggregateConstraintResult: AggregateConstraintResult,
                     acc: Tuple3<Double, Double, Long>): Tuple3<Double, Double, Long> {
        currentValue = aggregateConstraintResult.aggregate!!
        currentTimeStamp = aggregateConstraintResult.timestamp

        if (!includeInterval && waterMarkInterval != null) {
            val (startInterval, endInterval) = waterMarkInterval
            if (currentTimeStamp !in startInterval..endInterval) {
                acc.f0 = if (acc.f2 == 0L) {
                    currentValue
                } else {
                    acc.f0 + (currentValue - acc.f0) / (acc.f2 + 1)
                }
                acc.f1 = (acc.f1 * acc.f2 + (currentValue - acc.f0).pow(2)) / (acc.f2 + 1)
            }
        } else {
            acc.f0 = if (acc.f2 == 0L) {
                currentValue
            } else {
                acc.f0 + (currentValue - acc.f0) / (acc.f2 + 1)
            }

            acc.f1 = (acc.f1 * acc.f2 + (currentValue - acc.f0).pow(2)) / (acc.f2 + 1)
        }

        return Tuple3<Double, Double, Long>(acc.f0, acc.f1, acc.f2 + 1L)
    }

    /**
     * need to investigate how to only get mean and stdDev
     * of the last element in the accumulator
     */
    override fun getResult(acc: Tuple3<Double, Double, Long>): NormalStrategyResult {
        val mean = acc.f0
        val stdDev = sqrt(acc.f1)
        val upperBound = mean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = mean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        if (waterMarkInterval == null) {
            if (currentValue !in lowerBound..upperBound) {
                return NormalStrategyResult(currentValue, mean, stdDev)
            }
        } else {
            val (startInterval, endInterval) = waterMarkInterval
            if (currentValue !in lowerBound..upperBound && currentTimeStamp in startInterval..endInterval) {
                return NormalStrategyResult(currentValue, mean, stdDev)
            }
        }

        return NormalStrategyResult(currentValue, mean, stdDev)
    }

    override fun merge(acc0: Tuple3<Double, Double, Long>, acc1: Tuple3<Double, Double, Long>): Tuple3<Double, Double, Long> {
        return Tuple3<Double, Double, Long>(acc1.f0, acc1.f1, acc1.f2 + acc1.f2)
    }
}
