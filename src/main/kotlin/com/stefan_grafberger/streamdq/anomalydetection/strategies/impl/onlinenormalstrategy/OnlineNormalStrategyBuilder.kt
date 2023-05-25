package com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.onlinenormalstrategy

import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategyBuilder
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window

class OnlineNormalStrategyBuilder<W : Window> : AnomalyDetectionStrategyBuilder<W> {

    private lateinit var strategyWindowAssigner: WindowAssigner<Any?, W>
    private var lowerDeviationFactor: Double? = 3.0
    private var upperDeviationFactor: Double? = 3.0
    private var ignoreStartPercentage: Double = 0.1
    private var ignoreAnomalies: Boolean = true

    override fun withWindow(windowAssigner: WindowAssigner<Any?, W>): OnlineNormalStrategyBuilder<W> {
        this.strategyWindowAssigner = windowAssigner
        return this
    }

    fun withArguments(lowerDeviationFactor: Double,
                      upperDeviationFactor: Double,
                      ignoreStartPercentage: Double,
                      ignoreAnomalies: Boolean = true): OnlineNormalStrategyBuilder<W> {
        this.lowerDeviationFactor = lowerDeviationFactor
        this.upperDeviationFactor = upperDeviationFactor
        this.ignoreStartPercentage = ignoreStartPercentage
        this.ignoreAnomalies = ignoreAnomalies
        return this
    }

    override fun build(): AnomalyDetectionStrategy {
        return OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies, strategyWindowAssigner)
    }
}