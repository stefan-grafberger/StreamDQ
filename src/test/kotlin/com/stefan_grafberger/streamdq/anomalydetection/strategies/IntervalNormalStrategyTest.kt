package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.IntervalNormalStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class IntervalNormalStrategyTest {

    private lateinit var strategy: IntervalNormalStrategy
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
        val expectedAnomalies = MutableList(6) { Pair(it + 25, Anomaly(dataSeries[it + 25], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeries, Pair(25, 50))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenWhenLowerDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(null, 1.0)
        val expectedAnomalies = MutableList(6) { Pair(it * 2 + 20, Anomaly(dataSeries[it * 2 + 20], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeries, Pair(20, 31))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenWhenUpperDeviationFactorIsNUllExpectAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(1.0, null)
        val expectedAnomalies = MutableList(5) { Pair(it * 2 + 21, Anomaly(dataSeries[it * 2 + 21], 1.0)) }
        //when
        val anomalyResult = strategy.detectOnCache(dataSeries, Pair(10, 30))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenIntervalSpecifiedExpectIntervalValuesIgnoredAndAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(3.0, 3.0)
        val newDataSeries = mutableListOf(1.0, 1.0, 1.0, 1000.0, 500.0, 1.0)
        val expectedAnomalies = mutableListOf(
                Pair(3, Anomaly(newDataSeries[3], 1.0)),
                Pair(4, Anomaly(newDataSeries[3], 1.0)))
        //when
        val anomalyResult = strategy.detectOnCache(newDataSeries, Pair(3, 5))
        //then
        assertEquals(expectedAnomalies, anomalyResult)
    }

    @Test
    fun testDetectWhenTryToExcludeAllDataPointsExpectIllegalArgumentExceptionException() {
        //given
        strategy = IntervalNormalStrategy(3.0, 3.0)
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "Excluding values in searchInterval from calculation, but no more remaining values left to calculate mean/stdDev.",
                //when
                block = { val anomalyResult = strategy.detectOnCache(dataSeries) }
        )
    }

    @Test
    fun testDetectWhenAllDeviationFactorsAreMaximumExpectNoAnomaliesDetected() {
        //given
        strategy = IntervalNormalStrategy(Double.MAX_VALUE, Double.MAX_VALUE)
        //when
        val anomalyResult = strategy.detectOnCache(dataSeries, Pair(30, 50))
        //then
        assertTrue(anomalyResult.isEmpty())
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

    /**
     * Can not pass search interval for now
     */
    @Test
    fun testApplyWhenDataStreamComeExpectAnomalyStreamOutput() {
        //given
        strategy = IntervalNormalStrategy(1.0, 1.0, true)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = dataSeries.slice(20..30).map { value -> Anomaly(value, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalies)
        //when
        val actualAnomalyStream = strategy.apply(aggregateResultStream.second)
        val actualAnomalyList = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        expectedAnomalyStream.executeAndCollect().asSequence().toList()
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalyList)
    }

    @Test
    fun testApplyWhenDataStreamComeWithSpecifiedIntervalExpectAnomalyStreamOutput() {
        //given
        strategy = IntervalNormalStrategy(1.0, 1.0, true)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = dataSeries.slice(25..30).map { value -> Anomaly(value, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalies)
        val userDefinedSearchInterval = Pair(25,50)
        //when
        val actualAnomalyStream = strategy.apply(aggregateResultStream.second, userDefinedSearchInterval)
        val actualAnomalyList = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        expectedAnomalyStream.executeAndCollect().asSequence().toList()
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalyList)
    }
}
