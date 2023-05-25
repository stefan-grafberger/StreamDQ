package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.thresholdstrategy

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions.BoundMapFunction
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

class SimpleThresholdStrategy(
        private val lowerBound: Double = -Double.MAX_VALUE,
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
    override fun detect(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, AnomalyCheckResult>> {
        val (startInterval, endInterval) = searchInterval
        require(startInterval <= endInterval) { "The start of interval must be lower than the end" }
        val res: MutableCollection<Pair<Int, AnomalyCheckResult>> = mutableListOf()
        cachedStream.slice(startInterval until endInterval)
                .forEachIndexed { index, value ->
                    if (value < lowerBound || value > upperBound) {
                        res.add(Pair(index + startInterval, AnomalyCheckResult(value, true, 1.0)))
                    } else {
                        res.add(Pair(index + startInterval, AnomalyCheckResult(value, false, 1.0)))

                    }
                }
        return res
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>, waterMarkInterval: Pair<Long, Long>?): SingleOutputStreamOperator<AnomalyCheckResult> {
        var (startTimeStamp, endTimeStamp) = Pair(Long.MIN_VALUE, Long.MAX_VALUE)

        if (waterMarkInterval != null) {
            require(waterMarkInterval.first <= waterMarkInterval.second) {
                "The start of interval must be not bigger than the end"
            }
            startTimeStamp = waterMarkInterval.first
            endTimeStamp = waterMarkInterval.second
        }

        return dataStream
                .filter { data -> data.timestamp in startTimeStamp..endTimeStamp }
                .map { data -> AnomalyCheckResult(data.aggregate, false, confidence = 1.0) }
                .returns(AnomalyCheckResult::class.java)
                .map(BoundMapFunction(lowerBound, upperBound))
                .returns(AnomalyCheckResult::class.java)
    }
}
