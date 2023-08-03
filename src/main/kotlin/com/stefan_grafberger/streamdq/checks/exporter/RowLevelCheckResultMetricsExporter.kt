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

package com.stefan_grafberger.streamdq.checks.exporter

import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelConstraintResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class RowLevelCheckResultMetricsExporter<T> : RichMapFunction<RowLevelCheckResult<T>, RowLevelCheckResult<T>>() {
    private val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    private val countersByCheck: MutableMap<String, CounterPair> = mutableMapOf()

    override fun map(checkResults: RowLevelCheckResult<T>): RowLevelCheckResult<T> {
        checkResults.constraintResults
            ?.forEach {
                val pair = lookUpCounter(it)
                pair.increase(it.outcome ?: false)
            }

        logResults()
        return checkResults
    }

    private fun logResults() {
        countersByCheck.forEach {
            LOG.debug("[${this.hashCode()}] OK: [${it.value.successCounter.count}] NOK: [${it.value.failureCounter.count}] for [${it.key}]")
        }
    }

    private fun lookUpCounter(rowLevelConstraintResult: RowLevelConstraintResult): CounterPair {
        return countersByCheck.get(rowLevelConstraintResult.constraintName) ?: createCounterPair(rowLevelConstraintResult)
    }

    private fun createCounterPair(rowLevelConstraintResult: RowLevelConstraintResult): CounterPair {
        val successCounter = runtimeContext.metricGroup.counter(rowLevelConstraintResult.constraintName!! + "_success")
        val failureCounter = runtimeContext.metricGroup.counter(rowLevelConstraintResult.constraintName!! + "_failure")
        val pair = CounterPair(successCounter, failureCounter)

        countersByCheck[rowLevelConstraintResult.constraintName!!] = pair

        return pair
    }
}
