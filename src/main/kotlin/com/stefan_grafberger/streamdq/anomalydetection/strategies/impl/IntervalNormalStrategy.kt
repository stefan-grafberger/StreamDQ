package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.*
import org.nield.kotlinstatistics.standardDeviation

class IntervalNormalStrategy(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
        private val includeInterval: Boolean = false
) : AnomalyDetectionStrategy {

    init {
        require(lowerDeviationFactor != null || upperDeviationFactor != null) { "At least one factor has to be specified." }
        require(
                (lowerDeviationFactor ?: 1.0) >= 0 && (
                        upperDeviationFactor
                                ?: 1.0
                        ) >= 0
        ) { "Factors cannot be smaller than zero." }
    }

    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>> {

        val (startInterval, endInterval) = searchInterval
        val mean: Double
        val stdDev: Double
        val res: MutableCollection<Pair<Int, Anomaly>> = mutableListOf()

        require(startInterval <= endInterval) { "The start of interval must be lower than the end." }
        require(cachedStream.isNotEmpty()) { "Data stream is empty. Can't calculate mean/stdDev." }

        val searchIntervalLength = endInterval - startInterval

        require(includeInterval || searchIntervalLength > cachedStream.size) {
            "Excluding values in searchInterval from calculation, but no more remaining values left to calculate mean/stdDev."
        }

        if (includeInterval) {
            mean = cachedStream.average(); stdDev = cachedStream.standardDeviation()
        } else {
            val valuesBeforeInterval = cachedStream.slice(0..startInterval)
            val valuesAfterInterval = cachedStream.slice(endInterval..cachedStream.size)
            val dataSeriesWithoutInterval = valuesBeforeInterval + valuesAfterInterval
            mean = dataSeriesWithoutInterval.average()
            stdDev = dataSeriesWithoutInterval.standardDeviation()
        }

        val upperBound = mean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = mean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        cachedStream.slice(startInterval..endInterval)
                .forEachIndexed { index, value ->
                    if (value < lowerBound || value > upperBound) {
                        val detail = "[IntervalNormalStrategy]: data value $value is not in [$lowerBound, $upperBound]"
                        res.add(Pair(index, Anomaly(value, 1.0, detail)))
                    }
                }
        return res
    }

    override fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        TODO("Not yet implemented")
    }
}
