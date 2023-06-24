package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.data.ClickInfo
import com.stefan_grafberger.streamdq.data.ClickType
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle

@TestInstance(Lifecycle.PER_CLASS)
class AnalysisRunnerTest {

    @Test
    fun `test some RowLevelCheck`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck().isContainedIn(
            "categoryValue",
            listOf(ClickType.TypeA, ClickType.TypeB)
        )
        val verificationResult = AnalysisRunner()
            .addChecksToStream(rawStream, listOf(rowLevelCheck), listOf(), listOf(), env.config)

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(true, true, true, false, true)
        )
    }

    @Test
    fun `test nested check`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck()
            .isContainedIn("nestedInfo.nestedStringValue", listOf("a", "c", "d"))
        val verificationResult = AnalysisRunner()
            .addChecksToStream(rawStream, listOf(rowLevelCheck), listOf(), listOf(), env.config)

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(true, true, true, true, false)
        )
    }

    @Test
    fun `test multiple constraints check`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck()
            .isContainedIn("categoryValue", listOf(ClickType.TypeA, ClickType.TypeB))
            .isContainedIn("nestedInfo.nestedStringValue", listOf("a", "c", "d"))
        val verificationResult = AnalysisRunner()
            .addChecksToStream(rawStream, listOf(rowLevelCheck), listOf(), listOf(), env.config)

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(true, true, true, false, true)
        )
        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 1,
            arrayOf(true, true, true, true, false)
        )
    }

    @Test
    fun `test some AggregateCheck with multiple constraints`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val aggregateCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(3))
            .hasApproxCountDistinctBetween("userId", 4)
            .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3)
        val verificationResult = AnalysisRunner()
            .addChecksToStream(rawStream, listOf(), listOf(aggregateCheck), listOf(), env.config)

        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, aggregateCheck,
            doubleArrayOf(3.0, 4.0, 4.0), arrayOf(false, true, true)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, aggregateCheck,
            doubleArrayOf(1.0, 3.0, 5.0), arrayOf(true, true, false)
        )
    }

    @Test
    fun `test multiple checks multiple constraints`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelChecks = listOf(
            RowLevelCheck()
                .isContainedIn("categoryValue", listOf(ClickType.TypeA, ClickType.TypeB))
                .isContainedIn("nestedInfo.nestedStringValue", listOf("a", "c", "d")),
            RowLevelCheck()
                .isComplete("sessionId")
                .isComplete("nestedInfo.nestedIntValue")
        )
        val aggregateChecks = listOf(
            AggregateCheck()
                .onContinuousStreamWithTrigger(CountTrigger.of(3))
                .hasApproxCountDistinctBetween("userId", 4)
                .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3),
            AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .hasApproxQuantileBetween("intValue", 0.5, 7.0)
                .hasApproxQuantileBetween("nestedInfo.nestedDoubleValue", 0.9, null, 10.0)
        )
        val verificationResult = AnalysisRunner()
            .addChecksToStream(rawStream, rowLevelChecks, aggregateChecks, listOf(), env.config)

        val resultOne = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[0], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 0,
            arrayOf(true, true, true, false, true)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 1,
            arrayOf(true, true, true, true, false)
        )
        val resultTwo = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[1], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 0,
            arrayOf(true, true, true, true, false)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 1,
            arrayOf(true, true, false, true, true)
        )
        val resultThree = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[0], 3)
        TestUtils.assertAggregateConstraintResults(
            resultThree, 0, aggregateChecks[0],
            doubleArrayOf(3.0, 4.0, 4.0), arrayOf(false, true, true)
        )
        TestUtils.assertAggregateConstraintResults(
            resultThree, 1, aggregateChecks[0],
            doubleArrayOf(1.0, 3.0, 5.0), arrayOf(true, true, false)
        )
        val resultFour = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[1], 3)
        TestUtils.assertAggregateConstraintResults(
            resultFour, 0, aggregateChecks[1],
            doubleArrayOf(10.0, 8.0, 1.0), arrayOf(true, true, false)
        )
        TestUtils.assertAggregateConstraintResults(
            resultFour, 1, aggregateChecks[1],
            doubleArrayOf(8.6, 15.1, 4.19), arrayOf(true, false, true)
        )
    }

    @Test
    fun `test multiple checks with keyBy`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val keyedRawStream = rawStream.keyBy { clickInfo -> clickInfo.userId }

        val rowLevelChecks = listOf(
            RowLevelCheck()
                .isContainedIn("categoryValue", listOf(ClickType.TypeA, ClickType.TypeB))
                .isContainedIn("nestedInfo.nestedStringValue", listOf("a", "c", "d")),
            RowLevelCheck()
                .isComplete("sessionId")
                .isComplete("nestedInfo.nestedIntValue")
        )
        val aggregateChecks = listOf(
            AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .hasApproxCountDistinctBetween("userId", 4)
                .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3),
            AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .hasApproxQuantileBetween("intValue", 0.5, 7.0)
                .hasApproxQuantileBetween("nestedInfo.nestedDoubleValue", 0.9, null, 10.0)
        )
        val verificationResult = AnalysisRunner()
            .addChecksToStream(keyedRawStream, rowLevelChecks, aggregateChecks, listOf(), env.config)

        val resultOne = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[0], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 0,
            arrayOf(true, true, true, false, true)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 1,
            arrayOf(true, true, true, true, false)
        )
        val resultTwo = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[1], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 0,
            arrayOf(true, true, true, true, false)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 1,
            arrayOf(true, true, false, true, true)
        )
        val resultThree = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[0], 3)
        TestUtils.assertAggregateConstraintResults(
            resultThree, 0, aggregateChecks[0],
            doubleArrayOf(3.0, 4.0, 1.0), arrayOf(false, true, false)
        )
        TestUtils.assertAggregateConstraintResults(
            resultThree, 1, aggregateChecks[0],
            doubleArrayOf(3.0, 5.0, 1.0), arrayOf(true, false, true)
        )
        val resultFour = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[1], 3)
        TestUtils.assertAggregateConstraintResults(
            resultFour, 0, aggregateChecks[1],
            doubleArrayOf(10.0, 8.0, 1.0), arrayOf(true, true, false)
        )
        TestUtils.assertAggregateConstraintResults(
            resultFour, 1, aggregateChecks[1],
            doubleArrayOf(8.6, 15.1, 4.19), arrayOf(true, false, true)
        )
    }

    @Test
    fun `test multiple checks with keyBy without global aggregation`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val keyedRawStream = rawStream.keyBy { clickInfo -> clickInfo.userId }
        assert(keyedRawStream is KeyedStream<ClickInfo, String>)
        val rowLevelChecks = listOf(
            RowLevelCheck()
                .isContainedIn("categoryValue", listOf(ClickType.TypeA, ClickType.TypeB))
                .isContainedIn("nestedInfo.nestedStringValue", listOf("a", "c", "d")),
            RowLevelCheck()
                .isComplete("sessionId")
                .isComplete("nestedInfo.nestedIntValue")
        )
        val aggregateChecks = listOf(
            AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withGlobalResultComputedFromResultsPerKey(false)
                .hasApproxCountDistinctBetween("userId", 4)
                .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3),
            AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .withGlobalResultComputedFromResultsPerKey(false)
                .hasApproxQuantileBetween("intValue", 0.5, 7.0)
                .hasApproxQuantileBetween("nestedInfo.nestedDoubleValue", 0.9, null, 10.0)
        )
        val verificationResult = AnalysisRunner()
            .addChecksToStream(keyedRawStream, rowLevelChecks, aggregateChecks, listOf(), env.config)

        val resultOne = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[0], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 0,
            arrayOf(true, true, true, false, true)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultOne, rowLevelChecks[0], 1,
            arrayOf(true, true, true, true, false)
        )
        val resultTwo = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelChecks[1], 10)
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 0,
            arrayOf(true, true, true, true, false)
        )
        TestUtils.assertRowLevelConstraintResults(
            resultTwo, rowLevelChecks[1], 1,
            arrayOf(true, true, false, true, true)
        )

        // Recovering keyedStream is possible
        val keyedStream = DataStreamUtils.reinterpretAsKeyedStream(
            verificationResult.getResultsForCheck(aggregateChecks[0])!!
        ) { aggregateCheckResult -> aggregateCheckResult.partitionKeyValue }
        // Showing that it works
        val keyedStreamTest = keyedStream
            .reduce { aggregateCheckResult, _ -> aggregateCheckResult }
            .executeAndCollect()
            .asSequence().toList()
        assert(keyedStreamTest.size == 6) // If it is a keyedStream, reduce will only reduce each small partitions

        val expectedPartitions = arrayOf("userA", "UserB", "userA", "UserB", "userC", "UserB")

        val resultThree = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[0], 6)
        Assertions.assertArrayEquals(resultThree.map { it.partitionKeyValue }.toTypedArray(), expectedPartitions)
        TestUtils.assertAggregateConstraintResults(
            resultThree, 0, aggregateChecks[0],
            doubleArrayOf(1.0, 1.0, 1.0, 1.0, 1.0, 1.0), arrayOf(false, false, false, false, false, false)
        )
        TestUtils.assertAggregateConstraintResults(
            resultThree, 1, aggregateChecks[0],
            doubleArrayOf(3.0, 1.0, 1.0, 3.0, 1.0, 1.0), arrayOf(true, true, true, true, true, true)
        )
        val resultFour = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, aggregateChecks[1], 6)
        Assertions.assertArrayEquals(resultFour.map { it.partitionKeyValue }.toTypedArray(), expectedPartitions)
        TestUtils.assertAggregateConstraintResults(
            resultFour, 0, aggregateChecks[1],
            doubleArrayOf(20.0, 10.0, 3.0, 8.0, 9.0, 1.0), arrayOf(true, true, false, true, true, false)
        )
        TestUtils.assertAggregateConstraintResults(
            resultFour, 1, aggregateChecks[1],
            doubleArrayOf(8.6, 6.09, 2.29, 15.1, 9.6, 4.19), arrayOf(true, true, true, false, true, true)
        )
    }
}
