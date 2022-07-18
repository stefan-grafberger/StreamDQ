package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test

class KllSketchAggregateTest {

    @Test
    fun `test approx quantile with window`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                KllSketchAggregate(
                    rawStream.type, 0.5, null, null,
                    "test", "intValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(10.0, 8.0, 1.0))

        val hllStreamTwo = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(
                KllSketchAggregate(
                    rawStream.type, 0.9, null, null,
                    "test", "nestedInfo.nestedDoubleValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamTwo, 3, doubleArrayOf(8.6, 15.1, 4.19))
    }

    @Test
    fun `test continuous approx quantile with trigger`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val hllStreamOne = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                KllSketchAggregate(
                    rawStream.type, 0.5, null, null,
                    "test", "intValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllStreamOne, 3, doubleArrayOf(5.0, 10.0, 8.0))

        val hllTestStreamTwo = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(
                KllSketchAggregate(
                    rawStream.type, 0.9, null, null,
                    "test", "nestedInfo.nestedDoubleValue", env.config
                )
            )
        TestUtils.assertExpectedAggregates(hllTestStreamTwo, 3, doubleArrayOf(6.09, 9.6, 15.1))
    }
}
