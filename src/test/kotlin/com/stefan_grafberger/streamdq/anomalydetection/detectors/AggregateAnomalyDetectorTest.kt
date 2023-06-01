package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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
                .onCompleteness("nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(DetectionStrategy().onlineNormal(1.0, 1.0, 0.0))
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
                .onCompleteness("nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(DetectionStrategy().threshold(0.26, 0.9))
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

    @Test
    fun testDetectByAbsoluteChangeStrategyWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyCheck = AggregateAnomalyCheck()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val detector = aggregateAnomalyCheck
                .onCompleteness("nestedInfo.nestedIntValue")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(DetectionStrategy().absoluteChange(-0.1, 0.9, 1))
                .build()
        val expectedAnomalies = mutableListOf(AnomalyCheckResult(0.0046, true, 1.0),
                AnomalyCheckResult(1.0, true, 1.0))
        //when
        val actualAnomalies = detector
                .detectAnomalyStream(rawStream)
                .filter { result -> result.isAnomaly == true }
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }
}
