package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContinuousApproxQuantileTest {
    @Test
    fun `test ContinuousApproxQuantile works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val continuousCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(3))
            .hasApproxQuantileBetween("intValue", 0.5, 7.0)
            .hasApproxQuantileBetween("nestedInfo.nestedDoubleValue", 0.9, null, 10.0)
        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addAggregateCheck(continuousCheck)
            .build()

        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, continuousCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, continuousCheck,
            doubleArrayOf(5.0, 10.0, 8.0), arrayOf(false, true, true)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, continuousCheck,
            doubleArrayOf(6.09, 9.6, 15.1), arrayOf(true, true, false)
        )
    }

    @Test
    fun `test ContinuousApproxQuantile fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(100))
            .hasApproxQuantileBetween("invalid-path", 0.5)
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
