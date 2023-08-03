/**
 * Licensed to the University of Amsterdam (UvA) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The UvA licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stefan_grafberger.streamdq.checks.row

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigDecimal

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowValueInRangeTest {
    @Test
    fun `test RowValueInRange works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val rowLevelCheck = RowLevelCheck()
            .isInRange("timestamp", BigDecimal.valueOf(10_010), BigDecimal.valueOf(Long.MAX_VALUE))
            .isInRange("nestedInfo.nestedDoubleValue", BigDecimal.valueOf(3.0), BigDecimal.valueOf(8.0))

        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addRowLevelCheck(rowLevelCheck)
            .build()

        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 10)

        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 0,
            arrayOf(false, false, true, true, true)
        )
        TestUtils.assertRowLevelConstraintResults(
            result, rowLevelCheck, 1,
            arrayOf(true, false, true, true, false)
        )
    }

    @Test
    fun `test RowValueInRange fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = RowLevelCheck().isInRange(
            "invalid-path", BigDecimal.valueOf(0),
            BigDecimal.valueOf(10)
        )
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
