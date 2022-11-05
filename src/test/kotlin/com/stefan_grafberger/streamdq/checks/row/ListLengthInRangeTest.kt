package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListLengthInRangeTest {
    @Test
    fun `test ListLengthInRange works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck()
            .listLengthInRange("nestedInfo.nestedListValue",0,2)
            .listLengthInRange("nestedInfo.nestedListValue",0,3)
        val verificationResult = VerificationSuite()
            .onDataStream(rawStream, env.config)
            .addRowLevelCheck(rowLevelCheck)
            .build()

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(true, true, false, true, true, true, false, true, false, false)
        )
        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 1,
            arrayOf(true, true, true, true, true, true, true, true, true, true)
        )

    }

    @Test
    fun `test ListLengthInRange fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = RowLevelCheck().isComplete("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
