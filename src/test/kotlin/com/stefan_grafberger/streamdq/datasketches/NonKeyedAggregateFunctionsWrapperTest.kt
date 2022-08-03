package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.TestUtils.assertAggregateConstraintResultsWithNameAsString
import com.stefan_grafberger.streamdq.data.ClickInfo
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class NonKeyedAggregateFunctionsWrapperTest {

    @Test
    fun `test wrapper with window`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val aggregateFunctions = listOf(
            KllSketchAggregate(rawStream.type, 0.5, null, null, "test", "intValue", env.config),
            KllSketchAggregate(rawStream.type, 0.9, null, null, "test", "nestedInfo.nestedDoubleValue", env.config)
        )

        val functionWrapper = NonKeyedAggregateFunctionsWrapper<ClickInfo, Any>(aggregateFunctions)
        val aggregateFunctionResults = rawStream
            .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .aggregate(functionWrapper)

        val collectedResultOne = aggregateFunctionResults!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(3, collectedResultOne.size)

        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 0, "test",
            doubleArrayOf(10.0, 8.0, 1.0), arrayOf(true, true, true)
        )
        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 1, "test",
            doubleArrayOf(8.6, 15.1, 4.19), arrayOf(true, true, true)
        )
    }

    @Test
    fun `test wrapper with continuous`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val aggregateFunctions = listOf(
            KllSketchAggregate(rawStream.type, 0.5, null, null, "test", "intValue", env.config),
            KllSketchAggregate(rawStream.type, 0.9, null, null, "test", "nestedInfo.nestedDoubleValue", env.config)
        )

        val functionWrapper = NonKeyedAggregateFunctionsWrapper<ClickInfo, Any>(aggregateFunctions)
        val aggregateFunctionResults = rawStream
            .windowAll(GlobalWindows.create())
            .trigger(CountTrigger.of(3))
            .aggregate(functionWrapper)

        val collectedResultOne = aggregateFunctionResults!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(3, collectedResultOne.size)

        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 0, "test",
            doubleArrayOf(5.0, 10.0, 8.0), arrayOf(true, true, true)
        )
        assertAggregateConstraintResultsWithNameAsString(
            collectedResultOne, 1, "test",
            doubleArrayOf(6.09, 9.6, 15.1), arrayOf(true, true, true)
        )
    }
}
