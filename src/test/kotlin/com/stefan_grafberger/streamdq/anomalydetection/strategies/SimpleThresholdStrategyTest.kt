package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.thresholdstrategy.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SimpleThresholdStrategyTest {
    private lateinit var strategy: SimpleThresholdStrategy
    private val dataSeries = mutableListOf(-1.0, 2.0, 3.0, 0.5)

    @Test
    fun testDetectWhenIntervalSpecifiedExpectAnomaliesDetected() {
        //given
        val searchInterval = Pair(0, 3)
        strategy = SimpleThresholdStrategy(lowerBound = Double.MIN_VALUE, upperBound = 1.0)
        val expectedAnomalies = mutableListOf(
                Pair(0, AnomalyCheckResult(-1.0, true, 1.0)),
                Pair(1, AnomalyCheckResult(2.0, true, 1.0)),
                Pair(2, AnomalyCheckResult(3.0, true, 1.0)))
        //when
        val actualAnomalies = strategy
                .detect(dataSeries, searchInterval)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenNoIntervalSpecifiedExpectAllAnomaliesDetected() {
        //given
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        val expectedAnomalies = mutableListOf(
                Pair(1, AnomalyCheckResult(2.0, true, 1.0)),
                Pair(2, AnomalyCheckResult(3.0, true, 1.0)))
        //when
        val actualAnomalies = strategy
                .detect(dataSeries)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectWhenInputIsEmptyExpectEmptyAnomaliesOutput() {
        //given
        val newDataSeries = mutableListOf<Double>()
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        //when
        val actualAnomalies = strategy
                .detect(newDataSeries)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertTrue(actualAnomalies.isEmpty())
    }

    @Test
    fun testDetectWhenBothLowerAndUpperSpecifiedExpectAnomaliesDetected() {
        //given
        strategy = SimpleThresholdStrategy(-0.5, 1.0)
        val expectedAnomalies = mutableListOf(
                Pair(0, AnomalyCheckResult(-1.0, true, 1.0)),
                Pair(1, AnomalyCheckResult(2.0, true, 1.0)),
                Pair(2, AnomalyCheckResult(3.0, true, 1.0)))
        //when
        val actualAnomalies = strategy
                .detect(dataSeries)
                .filter { result -> result.second.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies)
    }

    @Test
    fun testDetectThresholdBoundIsNotOrderedExpectIllegalArgumentException() {
        //then
        assertFailsWith(exceptionClass = IllegalArgumentException::class,
                message = "The lower bound must be smaller or equal to the upper bound.",
                //when
                block = { strategy = SimpleThresholdStrategy(100.0, 10.0) }
        )
    }

    @Test
    fun testDetectWhenDataStreamComeExpectAnomalyStreamOutput() {
        //given
        val randomNum = Random(1)
        val newDataSeries = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }
        for (i in 20..30) {
            newDataSeries[i] = newDataSeries[i] + i + (i % 2 * -2 * i)
        }
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val expectedAnomalies = newDataSeries.filter { value -> value > 1.0 }.map { value -> AnomalyCheckResult(value, true, 1.0) }
        //when
        val actualAnomalyStream = strategy
                .detect(aggregateResultStream.second)
                .filter { result -> result.isAnomaly == true }
        val actualAnomalies = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalies,
                actualAnomalies)
    }
}
