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

package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The common interface for all anomaly detection strategies
 * Override in [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl]
 */
interface AnomalyDetectionStrategy {
    fun detect(dataStream: SingleOutputStreamOperator<AggregateConstraintResult>):
            SingleOutputStreamOperator<AnomalyCheckResult>
}
