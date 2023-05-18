package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.nield.kotlinstatistics.standardDeviation
import org.slf4j.LoggerFactory

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

    override fun detectOnStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        TODO("Not yet implemented")
    }

    override fun detectOnCache(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>> {

        val (startInterval, endInterval) = searchInterval
        val mean: Double
        val stdDev: Double
        val res: MutableCollection<Pair<Int, Anomaly>> = mutableListOf()

        require(startInterval <= endInterval) { "The start of interval must be lower than the end." }
        require(cachedStream.isNotEmpty()) { "Data stream is empty. Can't calculate mean/stdDev." }

        val searchIntervalLength = endInterval - startInterval

        require(includeInterval || searchIntervalLength < cachedStream.size) {
            "Excluding values in searchInterval from calculation, but no more remaining values left to calculate mean/stdDev."
        }

        if (includeInterval) {
            mean = cachedStream.average(); stdDev = cachedStream.standardDeviation()
        } else {
            val valuesBeforeInterval = cachedStream.slice(0 until startInterval)
            val valuesAfterInterval = cachedStream.slice(endInterval until cachedStream.size)
            val dataSeriesWithoutInterval = valuesBeforeInterval + valuesAfterInterval
            mean = dataSeriesWithoutInterval.average()
            stdDev = dataSeriesWithoutInterval.standardDeviation()
        }

        val upperBound = mean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = mean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        cachedStream.slice(startInterval until endInterval)
                .forEachIndexed { index, value ->
                    if (value < lowerBound || value > upperBound) {
                        val detail = "[IntervalNormalStrategy]: data value $value is not in [$lowerBound, $upperBound]"
                        res.add(Pair(index + startInterval, Anomaly(cachedStream[index + startInterval], 1.0, detail)))
                    }
                }
        return res
    }

    override fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        val logger = LoggerFactory.getLogger(IntervalNormalStrategy::class.java)
        val cachedStreamList = dataStream.executeAndCollect(1000)
                .mapNotNull { aggregateConstraintResult -> aggregateConstraintResult.aggregate }
        try {
            val cachedAnomalyResult = detectOnCache(cachedStreamList)
                    .map { resultPair -> resultPair.second }
            val env: StreamExecutionEnvironment = StreamExecutionEnvironment
                    .createLocalEnvironment()
            return env.fromCollection(cachedAnomalyResult)
        } catch (e: IllegalArgumentException) {
            logger.error("For InterValNormalStrategy, either specify the interval or set the includeInterval to true ")
            throw e
        }
    }

    override fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>, searchInterval: Pair<Int, Int>): SingleOutputStreamOperator<Anomaly> {
        val cachedStreamList = dataStream.executeAndCollect(1000)
                .mapNotNull { aggregateConstraintResult -> aggregateConstraintResult.aggregate }
        val cachedAnomalyResult = detectOnCache(cachedStreamList, searchInterval)
                .map { resultPair -> resultPair.second }
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironment()
        return env.fromCollection(cachedAnomalyResult)
    }
}
