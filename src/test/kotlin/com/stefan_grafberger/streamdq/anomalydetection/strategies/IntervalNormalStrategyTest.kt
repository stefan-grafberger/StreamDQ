package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.IntervalNormalStrategy
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

class IntervalNormalStrategyTest {

    private lateinit var strategy: IntervalNormalStrategy<GlobalWindow>
    private val randomNum = Random(1)
    private val dataSeries = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }

    init {
        for (i in 20..30) {
            dataSeries[i] = dataSeries[i] + i + (i % 2 * -2 * i)
        }
    }

    @Test
    fun testDetectWhenIntervalSpecifiedExpectAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(1.0, 1.0)
        val expectedAnomalies = MutableList(6) { Pair(it + 25, AnomalyCheckResult(dataSeries[it + 25], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeries, Pair(25, 50))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenWhenLowerDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(null, 1.0)
        val expectedAnomalies = MutableList(6) { Pair(it * 2 + 20, AnomalyCheckResult(dataSeries[it * 2 + 20], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeries, Pair(20, 31))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenWhenUpperDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(1.0, null)
        val expectedAnomalies = MutableList(5) { Pair(it * 2 + 21, AnomalyCheckResult(dataSeries[it * 2 + 21], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeries, Pair(10, 30))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenIntervalSpecifiedExpectIntervalValuesIgnoredAndAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(3.0, 3.0)
        val newDataSeries = mutableListOf(1.0, 1.0, 1.0, 1000.0, 500.0, 1.0)
        val expectedAnomalies = mutableListOf(
                Pair(3, AnomalyCheckResult(newDataSeries[3], true, 1.0)),
                Pair(4, AnomalyCheckResult(newDataSeries[4], true, 1.0)))
        //when
        val actualAnomalies = strategy
                .detect(newDataSeries, Pair(3, 5))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenTryToExcludeAllDataPointsExpectIllegalArgumentExceptionException() {
        //given
        strategy = IntervalNormalStrategy(3.0, 3.0)
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Excluding values in searchInterval from calculation, but no more remaining values left to calculate mean/stdDev.",
                //when
                block = { val actualAnomalies = strategy.detect(dataSeries) }
        )
    }

    @Test
    fun testDetectWhenAllDeviationFactorsAreMaximumExpectNoAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(Double.MAX_VALUE, Double.MAX_VALUE)
        //when
        val actualAnomalies = strategy
                .detect(dataSeries, Pair(30, 50))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertTrue(actualAnomalies.isEmpty())
    }

    @Test
    fun testCreateIntervalNormalStrategyWhenDeviationFactorIsNegativeExpectIllegalArgumentExceptionException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Factors cannot be smaller than zero.",
                //when
                block = { strategy = IntervalNormalStrategy(null, -3.0) }
        )
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Factors cannot be smaller than zero.",
                //when
                block = { strategy = IntervalNormalStrategy(-3.0, null) }
        )
    }

    @Test
    fun testCreateIntervalNormalStrategyWhenAllDeviationFactorIsNullExpectIllegalArgumentExceptionException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Factors cannot be smaller than zero.",
                //when
                block = { strategy = IntervalNormalStrategy(null, null) }
        )
    }

    @Test
    fun testDetectWhenIntervalIsIncludedExpectAnomalyStreamOutput() {
        //given
        strategy = IntervalNormalStrategy(1.0, 1.0, true, strategyWindowAssigner = GlobalWindows.create())
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = dataSeries.slice(20..30).map { value -> AnomalyCheckResult(value, true, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalies)
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second)

        val actualAnomalies = actualAnomalyStream.executeAndCollect()
                .asSequence()
                .toList()
                .filter { result -> result.isAnomaly == true }
        //then
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalies)
    }

    @Test
    fun testDetectWhenIntervalIsSpecifiedExpectAnomalyStreamOutput() {
        //given
        strategy = IntervalNormalStrategy(1.0, 1.0, strategyWindowAssigner = GlobalWindows.create())
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = dataSeries.slice(25..30).map { value -> AnomalyCheckResult(value, true, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalies)
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second, waterMarkInterval = Pair(1025L, 1040L))
                .filter { result -> result.isAnomaly == true }
        val actualAnomalies = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalies)
    }
}
