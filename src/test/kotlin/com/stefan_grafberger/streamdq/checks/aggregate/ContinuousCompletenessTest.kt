package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContinuousCompletenessTest {
    @Test
    fun `test ContinuousCompleteness works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val continuousCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(3))
            .hasCompletenessBetween("sessionId", 0.9)
            .hasCompletenessBetween("nestedInfo.nestedIntValue", null, 0.6)
        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addAggregateCheck(continuousCheck)
            .build()

        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, continuousCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, continuousCheck,
            doubleArrayOf(1.0, 0.83, 0.88), arrayOf(true, false, false)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, continuousCheck,
            doubleArrayOf(0.66, 0.66, 0.55), arrayOf(false, false, true)
        )
    }

    @Test
    fun `test ContinuousCompleteness fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(100))
            .hasCompletenessBetween("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
