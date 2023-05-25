package com.stefan_grafberger.streamdq.anomalydetection.strategies

import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.IntervalNormalStrategy
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

    fun <W : Window> onlineNormalWithCustomWindow(lowerDeviationFactor: Double? = 3.0,
                                                  upperDeviationFactor: Double? = 3.0,
                                                  ignoreStartPercentage: Double = 0.1,
                                                  ignoreAnomalies: Boolean = true,
                                                  windowAssigner: WindowAssigner<Any?, W>): OnlineNormalStrategy<W> {
        return OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies, windowAssigner)
    }

    fun intervalNormal(lowerDeviationFactor: Double? = 3.0,
                       upperDeviationFactor: Double? = 3.0,
                       includeInterval: Boolean = false): IntervalNormalStrategy<GlobalWindow> {
        return IntervalNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval, GlobalWindows.create())
    }

    fun <W : Window> intervalNormalWithCustomWindow(lowerDeviationFactor: Double? = 3.0, upperDeviationFactor: Double? = 3.0,
                                                    includeInterval: Boolean = false,
                                                    windowAssigner: WindowAssigner<Any?, W>): IntervalNormalStrategy<W> {
        return IntervalNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval, windowAssigner)
    }

    fun threshold(lowerBound: Double = -Double.MAX_VALUE, upperBound: Double): SimpleThresholdStrategy {
        return SimpleThresholdStrategy(lowerBound, upperBound)
    }
}
