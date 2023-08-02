package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.AbsoluteChangeStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AbsoluteChangeStrategyTest {

    private lateinit var strategy: AbsoluteChangeStrategy<GlobalWindow>
    private val dataSeriesList = MutableList(51) { 0.0 }

    init {
        for (i in 0..50) {
            if (i in 20..30) {
                if (i % 2 == 0) {
                    dataSeriesList[i] = i.toDouble()
                } else {
                    dataSeriesList[i] = -i.toDouble()
                }
            } else {
                dataSeriesList[i] = 1.0
            }
        }
    }

    @Test
    fun testDetectOnStreamWhenDataStreamComeExpectAnomalyStreamDetected() {
        //given
        strategy = AbsoluteChangeStrategy(-2.0, 2.0, strategyWindowAssigner = GlobalWindows.create())
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResultForAbsolute()
        val expectedAnomalies = dataSeriesList.slice(20..31).map { value -> AnomalyCheckResult(value, true) }
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second)
                .filter { result -> result.isAnomaly == true }
                .returns(AnomalyCheckResult::class.java)
        val actualAnomalies = actualAnomalyStream.executeAndCollect()
                .asSequence()
                .toList()
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenSecondOrderSpecifiedExpectAnomalyStreamDetected() {
        //given
        val data = mutableListOf(0.0, 1.0, 3.0, 6.0, 18.0, 72.0)
        strategy = AbsoluteChangeStrategy(maxRateIncrease = 8.0, order = 2, strategyWindowAssigner = GlobalWindows.create())
        val env = StreamExecutionEnvironment.createLocalEnvironment(1)
        val aggregateResultStream = env
                .fromCollection(data)
                .map { value -> AggregateConstraintResult(true, value, "test") }
                .returns(AggregateConstraintResult::class.java)
        val expectedAnomalies = mutableListOf(
                AnomalyCheckResult(18.0, true),
                AnomalyCheckResult(72.0, true))
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream)
                .filter { result -> result.isAnomaly == true }
                .returns(AnomalyCheckResult::class.java)
        val actualAnomalies = actualAnomalyStream.executeAndCollect()
                .asSequence()
                .toList()
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }
}