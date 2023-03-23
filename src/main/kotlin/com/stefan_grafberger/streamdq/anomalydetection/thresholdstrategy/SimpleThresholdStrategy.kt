package com.stefan_grafberger.streamdq.anomalydetection.thresholdstrategy

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetectionStrategy

class SimpleThresholdStrategy : AnomalyDetectionStrategy {
    override fun detect(dataStream: List<Double>, searchInterval: Pair<Int, Int>) {
        TODO("Not yet implemented")
    }
}