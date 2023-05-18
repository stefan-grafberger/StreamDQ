package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class SimpleThresholdStrategy(
        private val lowerBound: Double = - Double.MAX_VALUE,
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
    override fun detectOnCache(cachedStream: List<Double>, searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>> {
        val (startInterval, endInterval) = searchInterval
        require(startInterval <= endInterval) { "The start of interval must be lower than the end" }
        val res: MutableCollection<Pair<Int, Anomaly>> = mutableListOf()
        cachedStream.slice(startInterval until endInterval)
                .forEachIndexed { index, value ->
                    if (value < lowerBound || value > upperBound) {
                        val detail = "[SimpleThresholdStrategy]: data value $value is not in [$lowerBound, $upperBound]}"
                        res.add(Pair(index+startInterval, Anomaly(value, 1.0, detail)))
                    }
                }
        return res
    }

    override fun detectOnStream(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        TODO("Not yet implemented")
    }

    override fun apply(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<Anomaly> {
        val cachedStreamList = dataStream.executeAndCollect(1000)
                .mapNotNull { aggregateConstraintResult -> aggregateConstraintResult.aggregate }
        val cachedAnomalyResult = detectOnCache(cachedStreamList)
                .map { resultPair -> resultPair.second }
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment
                .createLocalEnvironment()
        return env.fromCollection(cachedAnomalyResult)
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