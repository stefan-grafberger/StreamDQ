package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

abstract class SimpleThresholdStrategy(
        private val lowerBound: Double,
        private val upperBound: Double) : AnomalyDetectionStrategy {

    init {
        require(lowerBound <= upperBound) { "The lower bound must be smaller or equal to the upper bound." }
    }

    /**
     * Search for anomalies in a stream of data
     *
     * @param cachedStream     The data contained in a List of Doubles
     * @param searchInterval The value range between which anomalies to be detected [a,b].
     * @return A list of Pairs with the indexes of anomalies in the interval and their corresponding wrapper object.
     */
    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>> {
        val (startInterval, endInterval) = searchInterval
        require(startInterval <= endInterval) { "The start of interval must be lower than the end" }
        val res: MutableCollection<Pair<Int, Anomaly>> = mutableListOf()
        cachedStream.slice(startInterval..endInterval)
                .forEachIndexed { index, value ->
                    if (value < lowerBound || value > upperBound) {
                        val detail = "[SimpleThresholdStrategy]: data value $value is not in [$lowerBound, $upperBound]}"
                        res.add(Pair(index, Anomaly(value, 1.0, detail)))
                    }
                }
        return res
    }

    override fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        TODO("Not yet implemented")
    }
}