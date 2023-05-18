package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
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
        val expected = mutableListOf(
                Pair(0, Anomaly(-1.0, 1.0)),
                Pair(1, Anomaly(2.0, 1.0)),
                Pair(2, Anomaly(3.0, 1.0)))
        //when
        val actualAnomalyResult = strategy.detectOnCache(dataSeries, searchInterval)
        //then
        assertEquals(expected, actualAnomalyResult)
    }

    @Test
    fun testDetectWhenNoIntervalSpecifiedExpectAllAnomaliesDetected() {
        //given
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        val expected = mutableListOf(
                Pair(1, Anomaly(2.0, 1.0)),
                Pair(2, Anomaly(3.0, 1.0)))
        //when
        val actualAnomalyResult = strategy.detectOnCache(dataSeries)
        //then
        assertEquals(expected, actualAnomalyResult)
    }

    @Test
    fun testDetectWhenInputIsEmptyExpectEmptyAnomaliesOutput() {
        //given
        val newDataSeries = mutableListOf<Double>()
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        //when
        val actualAnomalyResult = strategy.detectOnCache(newDataSeries)
        //then
        assertTrue(actualAnomalyResult.isEmpty())
    }

    @Test
    fun testDetectWhenBothLowerAndUpperSpecifiedExpectAnomaliesDetected() {
        //given
        strategy = SimpleThresholdStrategy(-0.5, 1.0)
        val expected = mutableListOf(
                Pair(0, Anomaly(-1.0, 1.0)),
                Pair(1, Anomaly(2.0, 1.0)),
                Pair(2, Anomaly(3.0, 1.0)))
        //when
        val actualAnomalyResult = strategy.detectOnCache(dataSeries)
        //then
        assertEquals(expected, actualAnomalyResult)
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
    fun testApplyWhenDataStreamComeExpectAnomalyStreamOutput() {
        //given
        val randomNum = Random(1)
        val newDataSeries = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }
        for (i in 20..30) {
            newDataSeries[i] = newDataSeries[i] + i + (i % 2 * -2 * i)
        }
        strategy = SimpleThresholdStrategy(upperBound = 1.0)
        val aggregateResultStream = TestDataUtils.createEnvAndGetAggregateResult()
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val expectedAnomalies = newDataSeries.filter { value -> value > 1.0 }.map { value -> Anomaly(value, 1.0) }
        val expectedAnomalyStream = environment.fromCollection(expectedAnomalies)
        //when
        val actualAnomalyStream = strategy.apply(aggregateResultStream.second)
        val actualAnomalyList = actualAnomalyStream.executeAndCollect().asSequence().toList()
        //then
        assertEquals(expectedAnomalyStream.executeAndCollect().asSequence().toList(),
                actualAnomalyList)
    }
}
