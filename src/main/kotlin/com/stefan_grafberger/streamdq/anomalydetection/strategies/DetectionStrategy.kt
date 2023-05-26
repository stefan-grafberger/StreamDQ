package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.OnlineNormalStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.windows.Window

class DetectionStrategy {

    fun onlineNormal(lowerDeviationFactor: Double? = 3.0,
                     upperDeviationFactor: Double? = 3.0,
                     ignoreStartPercentage: Double = 0.1,
                     ignoreAnomalies: Boolean = true): OnlineNormalStrategy<GlobalWindow> {
        return OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies, GlobalWindows.create())
    }

    /**
     * We can achieve intervalNormal by OnlineNormal Strategy with
     * a Sliding window of certain time
     */
    fun <W : Window> intervalNormal(lowerDeviationFactor: Double? = 3.0,
                                    upperDeviationFactor: Double? = 3.0,
                                    ignoreStartPercentage: Double = 0.1,
                                    ignoreAnomalies: Boolean = true,
                                    windowAssigner: WindowAssigner<Any?, W>): OnlineNormalStrategy<W> {
        return OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies, windowAssigner)
    }

    fun threshold(lowerBound: Double = -Double.MAX_VALUE, upperBound: Double): SimpleThresholdStrategy {
        return SimpleThresholdStrategy(lowerBound, upperBound)
    }
}
