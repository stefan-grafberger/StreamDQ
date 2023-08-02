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
import com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions.BoundMapFunction
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * Detects anomaly based on the current element value in the data stream
 * Check if values are in a specified range.
 *
 * @param lowerBound Lower bound of accepted range of values
 * @param upperBound Upper bound of accepted range of values
 */
data class SimpleThresholdStrategy(
        val lowerBound: Double = -Double.MAX_VALUE,
        val upperBound: Double
) : AnomalyDetectionStrategy {

    init {
        require(lowerBound <= upperBound) { "The lower bound must be smaller or equal to the upper bound." }
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>): SingleOutputStreamOperator<AnomalyCheckResult> {
        val (startTimeStamp, endTimeStamp) = Pair(Long.MIN_VALUE, Long.MAX_VALUE)
        return dataStream
                .filter { data -> data.timestamp in startTimeStamp..endTimeStamp }
                .map { data -> AnomalyCheckResult(data.aggregate, false) }
                .returns(AnomalyCheckResult::class.java)
                .map(BoundMapFunction(lowerBound, upperBound))
                .returns(AnomalyCheckResult::class.java)
    }
}
