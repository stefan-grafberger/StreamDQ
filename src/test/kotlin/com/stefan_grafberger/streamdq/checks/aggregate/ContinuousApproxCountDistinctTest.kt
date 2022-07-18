package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.TestUtils.assertAggregateConstraintResults
import com.stefan_grafberger.streamdq.TestUtils.assertFailsWithExpectedMessage
import com.stefan_grafberger.streamdq.TestUtils.collectAggregateResultStreamAndAssertLen
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContinuousApproxCountDistinctTest {

    @Test
    fun `test ContinuousApproxCountDistinct works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val continuousCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(3))
            .hasApproxCountDistinctBetween("userId", 4)
            .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3)
        val verificationResult = VerificationSuite()
            .onDataStream(rawStream, env.config)
            .addAggregateCheck(continuousCheck)
            .build()
        val result = collectAggregateResultStreamAndAssertLen(verificationResult, continuousCheck, 3)

        assertAggregateConstraintResults(
            result, 0, continuousCheck,
            doubleArrayOf(3.0, 4.0, 4.0), arrayOf(false, true, true)
        )
        assertAggregateConstraintResults(
            result, 1, continuousCheck,
            doubleArrayOf(1.0, 3.0, 5.0), arrayOf(true, true, false)
        )
    }

    @Test
    fun `test ContinuousApproxCountDistinct fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(100))
            .hasApproxCountDistinctBetween("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
