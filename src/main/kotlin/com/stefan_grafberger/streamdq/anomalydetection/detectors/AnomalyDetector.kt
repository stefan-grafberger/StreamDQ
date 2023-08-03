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

import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The interface for AnomalyDetector which is used
 * for windowing the data stream source at the beginning
 * and perform aggregate computation of data quality constraints
 * if needed
 *
 * @Override by [com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector]
 */
interface AnomalyDetector {
    fun <IN> detectAnomalyStream(dataStream: DataStream<IN>): SingleOutputStreamOperator<AnomalyCheckResult>
}
