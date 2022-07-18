package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test

class HllSketchUniquenessAggregateTest {

    @Test
    fun `test approx uniqueness with window`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                HllSketchUniquenessAggregate(
                    rawStream.type, null, null,
                    "test", "sessionId", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(0.4, 0.75, 1.0))

        val hllStreamTwo = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                HllSketchUniquenessAggregate(
                    rawStream.type, null, null,
                    "test", "nestedInfo.nestedStringValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamTwo, 3, doubleArrayOf(0.4, 1.0, 1.0))
    }

    @Test
    fun `test continuous approx uniqueness with trigger`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                HllSketchUniquenessAggregate(
                    rawStream.type, null, null,
                    "test", "sessionId", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(0.66, 0.5, 0.44))

        val hllTestStreamTwo = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                HllSketchUniquenessAggregate(
                    rawStream.type, null, null,
                    "test", "nestedInfo.nestedStringValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllTestStreamTwo, 3, doubleArrayOf(0.0, 0.33, 0.44))
    }
}
