package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.model.dto.NormalStrategyResultDto
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.OnlineNormalAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window
import kotlin.math.sqrt

/**
 * Detects anomaly based on the currently running mean and standard deviation
 * Can also exclude the anomalies from computation so that it will not affect
 * Assume the data is normal distributed
 *
 * @param lowerDeviationFactor  Catch anomalies if the data stream has a mean
 *                              smaller than mean - lowerDeviationFactor * stdDev
 * @param upperDeviationFactor  Catch anomalies if the data stream has a mean
 *  *                           bigger than mean + upperDeviationFactor * stdDev
 * @param ignoreStartPercentage Percentage of data stream after start in which no anomalies should
 *                              be detected (mean and stdDev are probably not representative before).
 * @param ignoreAnomalies       If set to true, ignores anomalous points in mean
 *                              and variance calculation.
 */
class OnlineNormalStrategy<W : Window>(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
        private val ignoreStartPercentage: Double = 0.1,
        private val ignoreAnomalies: Boolean = true,
        private val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
) : AnomalyDetectionStrategy {

    init {
        require(lowerDeviationFactor != null || upperDeviationFactor != null) { "At least one factor has to be specified." }
        require((lowerDeviationFactor ?: 1.0) >= 0 && (upperDeviationFactor
                ?: 1.0) >= 0) { "Factors cannot be smaller than zero." }
        require(ignoreStartPercentage in 0.0..1.0) { "Percentage of start values to ignore must be in interval [0, 1]." }
    }

    /**
     * incremental value computation
     * For each value, a new mean and standard deviation get computed based on the previous
     * calculation. To calculate the standard deviation, a helper variable Sn is used.
     * @searchInterval [a,b)
     */
    fun computeStatsAndAnomalies(
            cachedStream: List<Double>,
            searchInterval: Pair<Int, Int> = Pair(0, cachedStream.size))
            : List<NormalStrategyResultDto> {
        val resultList = mutableListOf<NormalStrategyResultDto>()
        var currentMean = 0.0
        var currentVariance = 0.0
        var sn = 0.0

        val numValuesToExclude = cachedStream.size * ignoreStartPercentage

        for (idx in cachedStream.indices) {
            val currentValue = cachedStream[idx]
            val lastMean = currentMean
            val lastVariance = currentVariance
            val lastSn = sn

            currentMean = if (idx == 0) {
                currentValue
            } else {
                lastMean + (1.0 / (idx + 1)) * (currentValue - lastMean)
            }

            sn += (currentValue - lastMean) * (currentValue - currentMean)
            currentVariance = sn / (idx + 1)
            val stdDev = sqrt(currentVariance)

            val upperBound = currentMean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
            val lowerBound = currentMean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

            val (searchStart, searchEnd) = searchInterval

            if (idx < numValuesToExclude ||
                    idx < searchStart ||
                    idx >= searchEnd || (currentValue in lowerBound..upperBound)) {
                resultList.add(NormalStrategyResultDto(currentValue, currentMean, stdDev, isAnomaly = false))
            } else {
                if (ignoreAnomalies) {
                    // AnomalyCheckResult doesn't affect mean and variance
                    currentMean = lastMean
                    currentVariance = lastVariance
                    sn = lastSn
                }
                resultList.add(NormalStrategyResultDto(currentValue, currentMean, stdDev, isAnomaly = true))
            }
        }
        return resultList
    }

    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, AnomalyCheckResult>> {
        val (startInterval, endInterval) = searchInterval
        val res: MutableCollection<Pair<Int, AnomalyCheckResult>> = mutableListOf()

        require(startInterval <= endInterval) { "The start of interval must be lower than the end." }

        computeStatsAndAnomalies(cachedStream)
                .slice(startInterval until endInterval)
                .forEachIndexed { index, result ->
                    if (result.isAnomaly) {
                        val upperBound = result.mean + (upperDeviationFactor
                                ?: Double.MAX_VALUE) * result.stdDev
                        val lowerBound = result.mean - (lowerDeviationFactor
                                ?: Double.MAX_VALUE) * result.stdDev
                        res.add(Pair(index + startInterval, AnomalyCheckResult(cachedStream[index + startInterval], true, 1.0)))
                    } else {
                        res.add(Pair(index + startInterval, AnomalyCheckResult(cachedStream[index + startInterval], false, 1.0)))
                    }
                }
        return res
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>,
                        waterMarkInterval: Pair<Long, Long>?)
            : SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(OnlineNormalAggregate(lowerDeviationFactor, upperDeviationFactor))
                .map { data -> AnomalyCheckResult(data.value, data.isAnomaly, 1.0) }
                .returns(AnomalyCheckResult::class.java)
    }
}
