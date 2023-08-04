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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigDecimal
import java.math.BigInteger

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
    fun testMapWhenFieldValueIsBigDecimalBiggerThanDoubleMaxValueExpectResultCorrect() {
        //arrange
        val bigDecimalList = listOf(BigDecimal(BigInteger.valueOf(Double.MAX_VALUE.toLong()), -2), BigDecimal.valueOf(0))
        val env = StreamExecutionEnvironment.createLocalEnvironment()
        val bigDecimalStream = env.fromCollection(bigDecimalList)
        val rowLevelCheck = RowLevelCheck()
                .isNonNegative("*")
        //act
        val verificationResult = VerificationSuite()
                .onDataStream(bigDecimalStream, env.config)
                .addRowLevelCheck(rowLevelCheck)
                .build()
        //assert
        val result = TestUtils.collectRowLevelResultStreamAndAssertLen(verificationResult, rowLevelCheck, 2)
        TestUtils.assertRowLevelConstraintResults(
                result, rowLevelCheck, 0,
                arrayOf(true, true)
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
