package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.Intervalnormalstrategy

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions.BoundMapFunction
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.IntervalNormalAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.nield.kotlinstatistics.standardDeviation

class IntervalNormalStrategy<W : Window>(
        private val lowerDeviationFactor: Double? = 3.0,
        private val upperDeviationFactor: Double? = 3.0,
        private val includeInterval: Boolean = false,
        private val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
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

    /**
     * by default, if waterMarkInterval is null,
     * then equals to include Interval of all data
     *
     * currently it is not a good approach since I did not
     * find a solution to calculate AnomalyCheckResult by the mean
     * and stdDEv of last element in the accumulator
     * of aggregate function yet
     */
    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>,
                        waterMarkInterval: Pair<Long, Long>?)
            : SingleOutputStreamOperator<AnomalyCheckResult> {
        val baselineData = dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(IntervalNormalAggregate(lowerDeviationFactor, upperDeviationFactor, includeInterval, waterMarkInterval))
                .executeAndCollect()
                .asSequence()
                .last()

        val (mean, stdDev) = Pair(baselineData.mean, baselineData.stdDev)
        val upperBound = mean + (upperDeviationFactor ?: Double.MAX_VALUE) * stdDev
        val lowerBound = mean - (lowerDeviationFactor ?: Double.MAX_VALUE) * stdDev

        return if (waterMarkInterval != null) {
            val (startTimeStamp, endTimeStamp) = waterMarkInterval
            dataStream
                    .filter { data -> data.timestamp in startTimeStamp..endTimeStamp }
                    .map { data -> AnomalyCheckResult(data.aggregate, false, 1.0) }
                    .returns(AnomalyCheckResult::class.java)
                    .map(BoundMapFunction(lowerBound, upperBound))
                    .returns(AnomalyCheckResult::class.java)
        } else {
            dataStream
                    .map { data -> AnomalyCheckResult(data.aggregate, false, 1.0) }
                    .returns(AnomalyCheckResult::class.java)
                    .map(BoundMapFunction(lowerBound, upperBound))
                    .returns(AnomalyCheckResult::class.java)
        }
    }

    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, AnomalyCheckResult>> {
        val (startInterval, endInterval) = searchInterval
        val mean: Double
        val stdDev: Double
        val res: MutableCollection<Pair<Int, AnomalyCheckResult>> = mutableListOf()

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
                        res.add(Pair(index + startInterval, AnomalyCheckResult(cachedStream[index + startInterval], true, 1.0)))
                    } else {
                        res.add(Pair(index + startInterval, AnomalyCheckResult(cachedStream[index + startInterval], false, 1.0)))
                    }
                }
        return res
    }
}
