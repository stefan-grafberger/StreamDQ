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

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.ConstraintResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class AggregateCheckResultsMetricsExporter<KEY> : RichMapFunction<AggregateCheckResult<KEY>, AggregateCheckResult<KEY>>() {
    // TODO: we should use generics here and merge both metric exporters into one

    private val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    private val metricHoldersByCheck: MutableMap<String, AggregateCounterTriplet> = mutableMapOf()

    override fun map(checkResults: AggregateCheckResult<KEY>): AggregateCheckResult<KEY> {
        checkResults.constraintResults
            ?.forEach {
                val pair = lookUpCounter(it)
                pair.measure(it.outcome ?: false, it.aggregate!!)
            }

        logResults()
        return checkResults
    }

    private fun logResults() {
        metricHoldersByCheck.forEach {
            LOG.debug("[${this.hashCode()}] OK: [${it.value.successCounter.count}] NOK: [${it.value.failureCounter.count}] for [${it.key}]")
        }
    }

    private fun lookUpCounter(constraintResult: ConstraintResult): AggregateCounterTriplet {
        return metricHoldersByCheck.get(constraintResult.constraintName) ?: createCounterPair(constraintResult)
    }

    private fun createCounterPair(constraintResult: ConstraintResult): AggregateCounterTriplet {
        val successCounter = runtimeContext.metricGroup.counter(constraintResult.constraintName!! + "_success")
        val failureCounter = runtimeContext.metricGroup.counter(constraintResult.constraintName!! + "_failure")
        val aggregateGauge = runtimeContext.metricGroup.gauge(constraintResult.constraintName!! + "_count", DoubleGauge(0.0))
        val triplet = AggregateCounterTriplet(successCounter, failureCounter, aggregateGauge)

        metricHoldersByCheck[constraintResult.constraintName!!] = triplet

        return triplet
    }
}
