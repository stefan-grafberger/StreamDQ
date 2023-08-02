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

package com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import org.apache.flink.api.common.functions.MapFunction

/**
 * BoundMapFunction is used to detect anomalies
 * based on the data's value in the stream and the
 * user-defined threshold
 *
 * This aggregate function is used in
 * [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy]
 *
 * @param lowerBound Upper bound of element value in the stream
 * @param upperBound Upper bound of element value in the stream
 */
class BoundMapFunction(
        private val lowerBound: Double,
        private val upperBound: Double
) : MapFunction<AnomalyCheckResult, AnomalyCheckResult> {
    override fun map(element: AnomalyCheckResult): AnomalyCheckResult {
        return if (element.value!! !in lowerBound..upperBound) {
            AnomalyCheckResult(element.value, true, element.detail)
        } else {
            AnomalyCheckResult(element.value, false, element.detail)
        }
    }
}