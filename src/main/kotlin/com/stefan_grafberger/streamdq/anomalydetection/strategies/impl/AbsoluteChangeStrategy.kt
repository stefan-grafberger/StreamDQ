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

package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.AbsoluteChangeAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Detects anomalies based on the data's absolute change(minus) in the stream
 * We introduce a concept of order, which is the order of the derivative. When
 * an element comes, order will self-decrease by 1 when the absolute change result
 * is computed once. The strategy will compute the difference between two consecutive
 * numbers in the stream until the order is 0 if possible.
 * Finally, compare the current absolut change rate with the customized rate bound.
 *
 * @param maxRateDecrease: Upper bound of accepted decrease (lower bound of increase).
 * @param maxRateIncrease: Upper bound of accepted growth.
 * @param order: order of the derivative
 */
class AbsoluteChangeStrategy<W : Window>(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1,
        private val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
) : AnomalyDetectionStrategy {

    init {
        require(maxRateDecrease <= maxRateIncrease) {
            "The maximal rate of increase has to be bigger than the maximal rate of decrease."
        }
        require(order >= 0) { "Order of derivative cannot be negative." }
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(AbsoluteChangeAggregate(maxRateDecrease, maxRateIncrease, order))
    }
}
