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

package com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AggregateAnomalyDetector(
        window: WindowAssigner<Any?, TimeWindow>,
        constraint: AggregateConstraint,
        strategy: AnomalyDetectionStrategy
) : AnomalyDetector {

    private var window: WindowAssigner<Any?, TimeWindow>
    var constraint: AggregateConstraint
    var strategy: AnomalyDetectionStrategy

    init {
        this.window = window
        this.constraint = constraint
        this.strategy = strategy
    }

    override fun <IN> detectAnomalyStream(
            dataStream: DataStream<IN>
    ): SingleOutputStreamOperator<AnomalyCheckResult> {
        return strategy.detect(dataStream
                .windowAll(window)
                .aggregate(constraint.getAggregateFunction(dataStream.type, dataStream.executionConfig)))
    }
}
