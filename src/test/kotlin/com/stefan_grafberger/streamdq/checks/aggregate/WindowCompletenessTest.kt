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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WindowCompletenessTest {
    @Test
    fun `test WindowCompleteness works`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val windowCheck = AggregateCheck().onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .hasCompletenessBetween("sessionId", 0.9)
            .hasCompletenessBetween("nestedInfo.nestedIntValue", null, 0.6)
        val verificationResult = VerificationSuite().onDataStream(rawStream, env.config)
            .addAggregateCheck(windowCheck)
            .build()

        val result = TestUtils.collectAggregateResultStreamAndAssertLen(verificationResult, windowCheck, 3)

        TestUtils.assertAggregateConstraintResults(
            result, 0, windowCheck,
            doubleArrayOf(0.8, 1.0, 1.0), arrayOf(false, true, true)
        )
        TestUtils.assertAggregateConstraintResults(
            result, 1, windowCheck,
            doubleArrayOf(0.8, 0.25, 0.0), arrayOf(false, true, true)
        )
    }

    @Test
    fun `test WindowCompleteness fails for invalid field references`() {
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val invalidFieldCheck = AggregateCheck()
            .onWindow(TumblingEventTimeWindows.of(Time.seconds(10)))
            .hasCompletenessBetween("invalid-path")
        val expectedExceptionMessage = "Invalid field expression \"invalid-path\"."
        TestUtils.assertFailsWithExpectedMessage(rawStream, env, invalidFieldCheck, expectedExceptionMessage)
    }
}
