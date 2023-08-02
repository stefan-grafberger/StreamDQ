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

package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * The interface for building AnomalyDetector
 * It is used for adding all the needed configurations
 * when user define anomaly check before using it in the
 * [com.stefan_grafberger.streamdq.VerificationSuite]
 *
 * @Override by [com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector]
 */
interface AnomalyCheck {
    fun build(): AnomalyDetector
    fun onCompleteness(keyExpressionString: String): AnomalyCheck
    fun onApproxUniqueness(keyExpressionString: String): AnomalyCheck
    fun onApproxCountDistinct(keyExpressionString: String): AnomalyCheck
    fun onApproxQuantile(keyExpressionString: String, quantile: Double): AnomalyCheck
    fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck
    fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck
}
