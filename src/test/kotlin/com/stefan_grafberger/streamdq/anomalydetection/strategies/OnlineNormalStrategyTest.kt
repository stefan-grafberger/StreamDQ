package com.stefan_grafberger.streamdq.anomalydetection.strategies;

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue


class OnlineNormalStrategyTest {
    private lateinit var strategy: OnlineNormalStrategy<GlobalWindow>
    private val randomNum = Random(1)
    private val dataSeriesList = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }

    init {
        for (i in 20..30) {
            dataSeriesList[i] = dataSeriesList[i] + i + (i % 2 * -2 * i)
        }
    }

    @Test
    fun testDetectWhenLowerDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(null, 1.5, strategyWindowAssigner = GlobalWindows.create())
        val (env, aggregateResultStream) = TestDataUtils.createEnvAndGetAggregateResult()
        val expectedAnomalies = MutableList(6) {
            AnomalyCheckResult(dataSeriesList[it * 2 + 20], true)
        }
        //when
        val actualAnomalies = strategy
                .detect(aggregateResultStream)
                .filter { result -> result.isAnomaly == true }
                .executeAndCollect().asSequence().toList().slice(5..10)
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenUpperDeviationIsFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(1.5, null, strategyWindowAssigner = GlobalWindows.create())
        val (env, aggregateResultStream) = TestDataUtils.createEnvAndGetAggregateResult()
        val expectedAnomalies = MutableList(5) {
            AnomalyCheckResult(dataSeriesList[it * 2 + 21], true)
        }
        //when
        val actualAnomalies = strategy
                .detect(aggregateResultStream)
                .filter { result -> result.isAnomaly == true }
                .executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenAllDeviationFactorsAreMaximumExpectNoAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(Double.MAX_VALUE, Double.MAX_VALUE, strategyWindowAssigner = GlobalWindows.create())
        val (env, aggregateResultStream) = TestDataUtils.createEnvAndGetAggregateResult()
        //when
        val actualAnomalies = strategy
                .detect(aggregateResultStream)
                .filter { result -> result.isAnomaly == true }
                .executeAndCollect().asSequence().toList()
        //then
        assertTrue(actualAnomalies.isEmpty())
    }

    @Test
    fun testCreateOnlineNormalStrategyWhenDeviationFactorIsNegativeExpectIllegalArgumentExceptionException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Factors cannot be smaller than zero.",
                //when
                block = { strategy = OnlineNormalStrategy(null, -3.0) }
        )
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Factors cannot be smaller than zero.",
                //when
                block = { strategy = OnlineNormalStrategy(-3.0, null) }
        )
    }

    @Test
    fun testDetectOnStreamWhenDataStreamComeExpectAnomalyStreamDetected() {
        //given
        strategy =
                OnlineNormalStrategy(3.5, 3.5, strategyWindowAssigner = GlobalWindows.create())
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies =
                dataSeriesList.slice(20..30).map { value -> AnomalyCheckResult(value, true) }
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second)
                .filter { result -> result.isAnomaly == true }
        val actualAnomalies = actualAnomalyStream.executeAndCollect()
                .asSequence()
                .toList()
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }
}
