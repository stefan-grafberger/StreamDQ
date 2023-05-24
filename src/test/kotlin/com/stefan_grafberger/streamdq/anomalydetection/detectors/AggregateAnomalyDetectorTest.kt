package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.model.metrics.Metric
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.IntervalNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.CompletenessConstraint
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AggregateAnomalyDetectorTest {

    private lateinit var aggregateAnomalyCheck: AnomalyCheck

    @Test
    fun testDetectAnomalyStreamWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyCheck = AggregateAnomalyCheck()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val detector = aggregateAnomalyCheck
                .withMetric(Metric.COMPLETENESS, "nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(OnlineNormalStrategy<GlobalWindow>(1.0, 1.0, 0.0, strategyWindowAssigner = GlobalWindows.create()))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(2, AnomalyCheckResult(0.0046, true, 1.0)),
                Pair(3, AnomalyCheckResult(1.0, true, 1.0))).map { element -> element.second }
        //when
        val actualAnomalies = detector
                .detectAnomalyStream(rawStream)
                .filter { result -> result.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectByIntervalStrategyWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyCheck = AggregateAnomalyCheck()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val detector = aggregateAnomalyCheck
                .withMetric(Metric.COMPLETENESS, "nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(IntervalNormalStrategy<GlobalWindow>(1.0, 1.0, true, strategyWindowAssigner = GlobalWindows.create()))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(2, AnomalyCheckResult(0.0046, true, 1.0)),
                Pair(3, AnomalyCheckResult(1.0, true, 1.0))).map { element -> element.second }
        //when
        val actualAnomalies = detector
                .detectAnomalyStream(rawStream)
                .filter { result -> result.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectBySimpleThresholdStrategyWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyCheck = AggregateAnomalyCheck()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val detector = aggregateAnomalyCheck
                .withMetric(Metric.COMPLETENESS, "nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(SimpleThresholdStrategy(lowerBound = 0.26, upperBound = 0.9))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(1, AnomalyCheckResult(0.25, true, 1.0)),
                Pair(2, AnomalyCheckResult(0.0046, true, 1.0)),
                Pair(3, AnomalyCheckResult(1.0, true, 1.0))).map { element -> element.second }
        //when
        val actualAnomalies = detector
                .detectAnomalyStream(rawStream)
                .filter { result -> result.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }
}
