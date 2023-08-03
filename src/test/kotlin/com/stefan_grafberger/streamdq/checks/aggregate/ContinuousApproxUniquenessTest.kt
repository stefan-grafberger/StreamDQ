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

package com.stefan_grafberger.streamdq.checks.aggregate

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContinuousApproxUniquenessTest {
    @Test
    fun `test ContinuousApproxUniqueness works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()

        val continuousCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(3))
            .hasApproxUniquenessBetween("sessionId", 0.5)
            .hasApproxUniquenessBetween("nestedInfo.nestedStringValue", null, 0.3)
        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addAggregateCheck(continuousCheck)
            .build()
        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, continuousCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, continuousCheck,
            doubleArrayOf(0.66, 0.5, 0.44), arrayOf(true, true, false)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, continuousCheck,
            doubleArrayOf(0.0, 0.33, 0.44), arrayOf(true, false, false)
        )
    }

    @Test
    fun `test ContinuousApproxUniqueness fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onContinuousStreamWithTrigger(CountTrigger.of(100))
            .hasApproxUniquenessBetween("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
