package com.stefan_grafberger.streamdq.anomalydetection.normalstrategy

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import org.nield.kotlinstatistics.standardDeviation

class IntervalNormalStrategy(
        private var lowerDeviationBound: Double? = 3.0,
        private var upperDeviationBound: Double? = 3.0,
        private var includeInterval: Boolean = false
) : AnomalyDetectionStrategy {

    init {
        require(lowerDeviationBound != null || upperDeviationBound != null) { "At least one factor has to be specified." }
        require((lowerDeviationBound ?: 1.0) >= 0 && (upperDeviationBound
                ?: 1.0) >= 0) { "Factors cannot be smaller than zero." }
    }

    override fun detect(dataStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>> {

        val (startInterval, endInterval) = searchInterval
        val mean: Double
        val stdDev: Double
        val res: MutableCollection<Pair<Int, Anomaly>> = mutableListOf()

        require(startInterval <= endInterval) { "The start of interval must be lower than the end." }
        require(dataStream.isNotEmpty()) { "Data stream is empty. Can't calculate mean/stdDev." }

        val searchIntervalLength = endInterval - startInterval

        require(includeInterval || searchIntervalLength > dataStream.size) {
            "Excluding values in searchInterval from calculation, but no more remaining values left to calculate mean/stdDev."
        }

        if (includeInterval) {
            mean = dataStream.average(); stdDev = dataStream.standardDeviation()
        } else {
            val valuesBeforeInterval = dataStream.slice(0..startInterval)
            val valuesAfterInterval = dataStream.slice(endInterval..dataStream.size)
            val dataSeriesWithoutInterval = valuesBeforeInterval + valuesAfterInterval
            mean = dataSeriesWithoutInterval.average()
            stdDev = dataSeriesWithoutInterval.standardDeviation()
        }

        val upperBound = mean + (upperDeviationBound ?: Double.MAX_VALUE) * stdDev
        val lowerBound = mean - (lowerDeviationBound ?: Double.MAX_VALUE) * stdDev

        dataStream.forEachIndexed { index, value ->
            if (value < lowerBound || value > upperBound) {
                val detail = "[SimpleThresholdStrategy]: data value $value is not in [$lowerBound, $upperBound]}"
                res.add(Pair(index, Anomaly(value, 1.0, detail)))
            }
        }
        return res
    }
}