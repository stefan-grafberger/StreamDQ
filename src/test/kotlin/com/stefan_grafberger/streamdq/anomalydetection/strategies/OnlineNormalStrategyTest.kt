package com.stefan_grafberger.streamdq.anomalydetection.strategies;

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue


class OnlineNormalStrategyTest {
    private lateinit var strategy: OnlineNormalStrategy
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
        val expectedAnomalies = MutableList(11) { Pair(it + 20, Anomaly(dataSeriesList[it + 20], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeriesList)
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenIntervalSpecifiedExpectAnomaliesInIntervalDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.2)
        val expectedAnomalies = MutableList(6) { Pair(it + 25, Anomaly(dataSeriesList[it + 25], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeriesList, Pair(25, 31))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenLowerDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(null, 1.5)
        val expectedAnomalies = MutableList(6) { Pair(it * 2 + 20, Anomaly(dataSeriesList[it * 2 + 20], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeriesList, Pair(20, 31))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenUpperDeviationIsFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(1.5, null)
        val expectedAnomalies = MutableList(5) { Pair(it * 2 + 21, Anomaly(dataSeriesList[it * 2 + 21], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeriesList)
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenCachedDataStreamIsEmptyExpectEmptyOutput() {
        //given
        strategy = OnlineNormalStrategy(1.5, null)
        val emptySeries: MutableList<Double> = mutableListOf()
        //when
        val anomalyResult = strategy.detectOnCache(emptySeries)
        //then
        assertTrue(anomalyResult.isEmpty())
    }

    @Test
    fun testDetectWhenAllDeviationFactorsAreMaximumExpectNoAnomaliesDetected() {
        //given
        strategy = OnlineNormalStrategy(Double.MAX_VALUE, Double.MAX_VALUE)
        //when
        val anomalyResult = strategy.detectOnCache(dataSeriesList)
        //then
        assertTrue(anomalyResult.isEmpty())
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
    fun testDetectOnCacheWhenDataStreamComeExpectAnomalyStreamDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.0)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalyList = dataSeriesList.slice(20..30).map { value -> Anomaly(value, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalyList)
        //when
        val actualAnomalyStream = strategy.apply(aggregateResultStream.second)
        val actualAnomalyList = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalyList)
    }

    @Test
    fun testDetectOnStreamWhenDataStreamComeExpectAnomalyStreamDetected() {
        //given
        strategy = OnlineNormalStrategy(3.5, 3.5, 0.0)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalyList = dataSeriesList.slice(20..30).map { value -> Anomaly(value, 1.0) }
        //when
        val anomalyResult = strategy.detectOnStream(aggregateResultStream.second)
        val anomalyResultList = anomalyResult.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalyList, anomalyResultList)
    }
}
