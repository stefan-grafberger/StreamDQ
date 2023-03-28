package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.normalstrategy.IntervalNormalStrategy
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class IntervalNormalStrategyTest {
    @Test
    fun test() {
        val lowerDeviationBound = 1.0
        val upperDeviationBound = 4.0
        val includeInterval = true
        val data = listOf(1.0, 3.0, 4.0, 5.0, 9.0)
        val searchInterval: Pair<Int, Int> = Pair(0, 100)

        val intervalS = IntervalNormalStrategy(lowerDeviationBound, upperDeviationBound, includeInterval)
        val list = intervalS.detect(data, searchInterval)

        Assertions.assertEquals(0, list.iterator().next().first)
        Assertions.assertEquals("[SimpleThresholdStrategy]: data value 1.0 is not in [1.433520605161735, 16.26591757935306]", list.iterator().next().second.detail)
    }
}