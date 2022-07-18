package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WindowApproxUniquenessTest {
    @Test
    fun `test WindowApproxUniqueness works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val windowCheck = AggregateCheck()
            .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .hasApproxUniquenessBetween("sessionId", 0.5)
            .hasApproxUniquenessBetween("nestedInfo.nestedStringValue", null, 0.5)

        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addAggregateCheck(windowCheck)
            .build()
        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, windowCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, windowCheck,
            doubleArrayOf(0.4, 0.75, 1.0), arrayOf(false, true, true)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, windowCheck,
            doubleArrayOf(0.4, 1.0, 1.0), arrayOf(true, false, false)
        )
    }

    @Test
    fun `test WindowApproxUniqueness fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onWindow(TumblingEventTimeWindows.of(Time.seconds(10)))
            .hasApproxUniquenessBetween("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
