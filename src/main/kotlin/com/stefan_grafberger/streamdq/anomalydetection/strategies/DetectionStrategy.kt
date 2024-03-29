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

import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.AbsoluteChangeStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.RelativeRateOfChangeStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * A class for centering the anomaly detection strategies so that
 * anomaly detection is more user-friendly
 */
class DetectionStrategy {

    fun onlineNormal(
            lowerDeviationFactor: Double? = 3.0,
            upperDeviationFactor: Double? = 3.0,
            ignoreAnomalies: Boolean = true
    ): OnlineNormalStrategy<GlobalWindow> {
        return OnlineNormalStrategy(
                lowerDeviationFactor,
                upperDeviationFactor,
                ignoreAnomalies,
                GlobalWindows.create()
        )
    }

    /**
     * We can achieve intervalNormal by OnlineNormal Strategy with
     * a Sliding window of certain time
     */
    fun <W : Window> intervalNormal(
            lowerDeviationFactor: Double? = 3.0,
            upperDeviationFactor: Double? = 3.0,
            ignoreAnomalies: Boolean = true,
            windowAssigner: WindowAssigner<Any?, W>
    ): OnlineNormalStrategy<W> {
        return OnlineNormalStrategy(
                lowerDeviationFactor,
                upperDeviationFactor,
                ignoreAnomalies,
                windowAssigner
        )
    }

    fun threshold(
            lowerBound: Double = -Double.MAX_VALUE,
            upperBound: Double
    ): SimpleThresholdStrategy {
        return SimpleThresholdStrategy(lowerBound, upperBound)
    }

    fun absoluteChange(
            maxRateDecrease: Double = -Double.MAX_VALUE,
            maxRateIncrease: Double = Double.MAX_VALUE,
            order: Int = 1
    ): AbsoluteChangeStrategy<GlobalWindow> {
        return AbsoluteChangeStrategy(
                maxRateDecrease, maxRateIncrease, order,
                GlobalWindows.create()
        )
    }

    fun <W : Window> absoluteChangeWithCustomWindow(
            maxRateDecrease: Double = -Double.MAX_VALUE,
            maxRateIncrease: Double = Double.MAX_VALUE,
            order: Int = 1,
            strategyWindowAssigner: WindowAssigner<Any?, W>?
    ): AbsoluteChangeStrategy<W> {
        return AbsoluteChangeStrategy(
                maxRateDecrease, maxRateIncrease, order,
                strategyWindowAssigner
        )
    }

    fun relativeRateOfChange(
            maxRateDecrease: Double = -Double.MAX_VALUE,
            maxRateIncrease: Double = Double.MAX_VALUE,
            order: Int = 1
    ): RelativeRateOfChangeStrategy<GlobalWindow> {
        return RelativeRateOfChangeStrategy(
                maxRateDecrease, maxRateIncrease, order,
                GlobalWindows.create()
        )
    }

    fun <W : Window> relativeRateOfChangeWithCustomWindow(
            maxRateDecrease: Double = -Double.MAX_VALUE,
            maxRateIncrease: Double = Double.MAX_VALUE,
            order: Int = 1,
            strategyWindowAssigner: WindowAssigner<Any?, W>?
    ): RelativeRateOfChangeStrategy<W> {
        return RelativeRateOfChangeStrategy(
                maxRateDecrease, maxRateIncrease, order,
                strategyWindowAssigner
        )
    }
}
