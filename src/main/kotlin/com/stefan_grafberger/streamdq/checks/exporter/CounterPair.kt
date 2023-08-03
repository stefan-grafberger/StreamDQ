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

import org.apache.flink.metrics.Counter
import org.apache.flink.metrics.Gauge

data class CounterPair(
    val successCounter: Counter,
    val failureCounter: Counter
) {
    fun increase(success: Boolean) {
        if (success) successCounter.inc()
        else failureCounter.inc()
    }
}

data class AggregateCounterTriplet(
    val successCounter: Counter,
    val failureCounter: Counter,
    val aggregateGauge: DoubleGauge
) {
    fun measure(success: Boolean, aggregateCount: Double) {
        aggregateGauge.value = aggregateCount
        if (success) successCounter.inc()
        else failureCounter.inc()
    }
}

data class DoubleGauge(var value: Double) : Gauge<Double> {
    override fun getValue() = value
}
