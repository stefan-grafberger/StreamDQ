package com.stefan_grafberger.streamdq.anomalydetection

import com.stefan_grafberger.streamdq.anomalydetection.model.Anomaly

/**
 * The common interface for all anomaly detection strategies
 */
interface AnomalyDetectionStrategy {
    fun detect(dataStream: List<Double>,
               searchInterval: Pair<Int, Int>): MutableCollection<Pair<Int, Anomaly>>
}