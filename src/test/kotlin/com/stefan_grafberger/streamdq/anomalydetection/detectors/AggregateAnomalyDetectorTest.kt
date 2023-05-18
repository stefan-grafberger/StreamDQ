package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectorBuilder
import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyDetectorBuilder
import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
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
    fun testDetectOnCacheAnomalyStreamWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
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
        val actualAnomaly = detector.detectAnomalyStreamByCache(aggregateStream)
        //then
        println(actualAnomaly.executeAndCollect().asSequence().toList().map { ele -> ele.value })
        assertEquals(expectedAnomalies, actualAnomaly.executeAndCollect().asSequence().toList())
    }

    @Test
    fun testDetectOnStreamAnomalyStreamWhenAbnormalClickStreamComeExpectAnomalyStreamDetected() {
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
        val actualAnomaly = detector.detectAnomalyStream(aggregateStream)
        //then
        assertEquals(expectedAnomalies, actualAnomaly.executeAndCollect().asSequence().toList())
    }
}
