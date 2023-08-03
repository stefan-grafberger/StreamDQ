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

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.*
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class AggregateAnomalyCheck : AnomalyCheck {

    private lateinit var window: WindowAssigner<Any?, TimeWindow>
    private lateinit var strategy: AnomalyDetectionStrategy
    private lateinit var metric: AggregateConstraint

    override fun build(): AggregateAnomalyDetector {
        return AggregateAnomalyDetector(window, metric, strategy)
    }

    override fun withWindow(windowAssigner: WindowAssigner<Any?, TimeWindow>): AnomalyCheck {
        this.window = windowAssigner
        return this
    }

    override fun onCompleteness(keyExpressionString: String): AnomalyCheck {
        this.metric = CompletenessConstraint(keyExpressionString)
        return this
    }

    override fun onApproxUniqueness(keyExpressionString: String): AnomalyCheck {
        this.metric = ApproxUniquenessConstraint(keyExpressionString)
        return this
    }

    override fun onApproxCountDistinct(keyExpressionString: String): AnomalyCheck {
        this.metric = ApproxCountDistinctConstraint(keyExpressionString)
        return this
    }

    override fun onApproxQuantile(keyExpressionString: String, quantile: Double): AnomalyCheck {
        this.metric = ApproxQuantileConstraint(keyExpressionString, quantile)
        return this
    }

    override fun withStrategy(strategy: AnomalyDetectionStrategy): AnomalyCheck {
        this.strategy = strategy
        return this
    }
}
