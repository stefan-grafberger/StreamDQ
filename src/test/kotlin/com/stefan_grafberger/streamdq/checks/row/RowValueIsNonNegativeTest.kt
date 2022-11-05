package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowValueIsNonNegativeTest {
    @Test
    fun `test RowValueIsNonNegative works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck()
            .isNonNegative("timestamp")
        val verificationResult = VerificationSuite()
            .onDataStream(rawStream, env.config)
            .addRowLevelCheck(rowLevelCheck)
            .build()

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(true, true, true, true, true, true, true, true, true, true)
        )

    }

    @Test
    fun `test RowValueIsNonNegative fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = RowLevelCheck().isComplete("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
