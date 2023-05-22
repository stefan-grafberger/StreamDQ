package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectorBuilder
import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyDetectorBuilder
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.IntervalNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.aggregate.CompletenessConstraint
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class AggregateAnomalyDetectorTest {

    private lateinit var aggregateAnomalyDetectorBuilder: AnomalyDetectorBuilder

    @Test
    fun testDetectAnomalyStreamByCacheStreamWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyDetectorBuilder = AggregateAnomalyDetectorBuilder()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val constraint = CompletenessConstraint("aggregate")
        val detector = aggregateAnomalyDetectorBuilder
                .withAggregatedConstraint(constraint)
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(OnlineNormalStrategy(1.0, 1.0, 0.0))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(2, Anomaly(0.0046, 1.0)),
                Pair(3, Anomaly(1.0, 1.0))).map { element -> element.second }
        val aggregateStream = env.fromCollection(
                rawStream.executeAndCollect()
                        .asSequence()
                        .toList()
                        .map { element -> AggregateConstraintResult(true, element.nestedInfo.nestedIntValue?.toDouble(), "completeness", element.timestamp) })
        //when
        val actualAnomalies = detector.detectAnomalyStreamByCache(aggregateStream)
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectAnomalyStreamWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyDetectorBuilder = AggregateAnomalyDetectorBuilder()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val constraint = CompletenessConstraint("aggregate")
        val detector = aggregateAnomalyDetectorBuilder
                .withAggregatedConstraint(constraint)
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(OnlineNormalStrategy(1.0, 1.0, 0.0))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(2, Anomaly(0.0046, 1.0)),
                Pair(3, Anomaly(1.0, 1.0))).map { element -> element.second }
        val aggregateStream = env.fromCollection(
                rawStream.executeAndCollect()
                        .asSequence()
                        .toList()
                        .map { element -> AggregateConstraintResult(true, element.nestedInfo.nestedIntValue?.toDouble(), "completeness", element.timestamp) })
        //when
        val actualAnomalies = detector.detectAnomalyStream(aggregateStream)
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectByIntervalStrategyWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyDetectorBuilder = AggregateAnomalyDetectorBuilder()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val constraint = CompletenessConstraint("aggregate")
        val detector = aggregateAnomalyDetectorBuilder
                .withAggregatedConstraint(constraint)
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(IntervalNormalStrategy(1.0, 1.0, true))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(2, Anomaly(0.0046, 1.0)),
                Pair(3, Anomaly(1.0, 1.0))).map { element -> element.second }
        val aggregateStream = env.fromCollection(
                rawStream.executeAndCollect()
                        .asSequence()
                        .toList()
                        .map { element -> AggregateConstraintResult(true, element.nestedInfo.nestedIntValue?.toDouble(), "completeness", element.timestamp) })
        //when
        val actualAnomalies = detector.detectAnomalyStream(aggregateStream)
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectBySimpleThresholdStrategyWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
        //given
        aggregateAnomalyDetectorBuilder = AggregateAnomalyDetectorBuilder()
        val (env, rawStream) = TestDataUtils.createEnvAndGetAbnormalClickStream()
        val constraint = CompletenessConstraint("aggregate")
        val detector = aggregateAnomalyDetectorBuilder
                .withAggregatedConstraint(constraint)
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withStrategy(SimpleThresholdStrategy(lowerBound = 0.26, upperBound = 0.9))
                .build()
        val expectedAnomalies = mutableListOf(
                Pair(1, Anomaly(0.25, 1.0)),
                Pair(2, Anomaly(0.0046, 1.0)),
                Pair(3, Anomaly(1.0, 1.0))).map { element -> element.second }
        val aggregateStream = env.fromCollection(
                rawStream.executeAndCollect()
                        .asSequence()
                        .toList()
                        .map { element -> AggregateConstraintResult(true, element.nestedInfo.nestedIntValue?.toDouble(), "completeness", element.timestamp) })
        //when
        val actualAnomalies = detector.detectAnomalyStream(aggregateStream)
        //then
        assertEquals(expectedAnomalies, actualAnomalies.executeAndCollect().asSequence().toList())
    }
}
