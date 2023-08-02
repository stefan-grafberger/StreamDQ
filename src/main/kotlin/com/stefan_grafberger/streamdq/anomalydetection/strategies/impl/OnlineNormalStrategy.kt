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
import com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions.OnlineNormalAggregate
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * Detects anomaly based on the currently running mean and standard deviation
 * Assume the data is normal distributed
 *
 * @param lowerDeviationFactor   Catch anomalies if the data stream has a mean
 *                               smaller than mean - lowerDeviationFactor * stdDev
 * @param upperDeviationFactor   Catch anomalies if the data stream has a mean
 *                               bigger than mean + upperDeviationFactor * stdDev
 * @param strategyWindowAssigner Flink window assigner used for windowing the data stream
 *                               and perform aggregate computation to detect anomalies
 *                               based on windowed stream
 */
data class OnlineNormalStrategy<W : Window>(
        val lowerDeviationFactor: Double? = 3.0,
        val upperDeviationFactor: Double? = 3.0,
        val ignoreAnomalies: Boolean = true,
        val strategyWindowAssigner: WindowAssigner<Any?, W>? = null
) : AnomalyDetectionStrategy {

    init {
        require(lowerDeviationFactor != null || upperDeviationFactor != null) { "At least one factor has to be specified." }
        require(
                (lowerDeviationFactor ?: 1.0) >= 0 && (upperDeviationFactor
                        ?: 1.0) >= 0
        ) { "Factors cannot be smaller than zero." }
    }

    override fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>)
            : SingleOutputStreamOperator<AnomalyCheckResult> {
        return dataStream
                .windowAll(strategyWindowAssigner)
                .trigger(CountTrigger.of(1))
                .aggregate(OnlineNormalAggregate(lowerDeviationFactor, upperDeviationFactor))
                .map { data -> AnomalyCheckResult(data.value, data.isAnomaly) }
                .returns(AnomalyCheckResult::class.java)
    }
}
