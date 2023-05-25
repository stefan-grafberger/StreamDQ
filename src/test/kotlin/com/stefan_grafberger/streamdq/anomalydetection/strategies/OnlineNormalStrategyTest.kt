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
    fun testDetectWhenNoIntervalSpecifiedExpectAllAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.2)
        val expectedAnomalies = MutableList(11) { Pair(it + 20, AnomalyCheckResult(dataSeriesList[it + 20], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeriesList)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenIntervalSpecifiedExpectAnomaliesInIntervalDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.2)
        val expectedAnomalies = MutableList(6) { Pair(it + 25, AnomalyCheckResult(dataSeriesList[it + 25], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeriesList, Pair(25, 31))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenLowerDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(null, 1.5)
        val expectedAnomalies = MutableList(6) { Pair(it * 2 + 20, AnomalyCheckResult(dataSeriesList[it * 2 + 20], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeriesList, Pair(20, 31))
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenUpperDeviationIsFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(1.5, null)
        val expectedAnomalies = MutableList(5) { Pair(it * 2 + 21, AnomalyCheckResult(dataSeriesList[it * 2 + 21], true, 1.0)) }
        //when
        val actualAnomalies = strategy
                .detect(dataSeriesList)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenCachedDataStreamIsEmptyExpectEmptyOutput() {
        //given
        strategy = OnlineNormalStrategy(1.5, null)
        val emptySeries: MutableList<Double> = mutableListOf()
        //when
        val actualAnomalies = strategy
                .detect(emptySeries)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertTrue(actualAnomalies.isEmpty())
    }

    @Test
    fun testDetectWhenAllDeviationFactorsAreMaximumExpectNoAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(Double.MAX_VALUE, Double.MAX_VALUE)
        //when
        val actualAnomalies = strategy
                .detect(dataSeriesList)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertTrue(actualAnomalies.isEmpty())
    }

    /**
     * better to find a library which is able to do
     * incremental computation of mean and stdDev
     */
    @Test
    fun testVarianceAndMeanCalculationWhenComputeStatsExpectCorrect() {
        //given
        strategy = OnlineNormalStrategy()
        //when
        val lastDataPoint = strategy.computeStatsAndAnomalies(dataSeriesList).last()
        //then
        assertEquals(1.071812328136762, lastDataPoint.value)
        assertEquals(0.06230422033200966, lastDataPoint.mean)
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
    fun testCreateOnlineNormalStrategyWhenIgnoreStartPercentageNotInRangeExpectIllegalArgumentExceptionException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Percentage of start values to ignore must be in interval [0, 1].",
                //when
                block = { strategy = OnlineNormalStrategy(null, 3.0, 1.5) }
        )
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Percentage of start values to ignore must be in interval [0, 1].",
                //when
                block = { strategy = OnlineNormalStrategy(null, 3.0, -1.0) }
        )
    }

    @Test
    fun testDetectOnStreamWhenDataStreamComeExpectAnomalyStreamDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.0, strategyWindowAssigner = GlobalWindows.create())
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = dataSeriesList.slice(20..30).map { value -> AnomalyCheckResult(value, true, 1.0) }
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
