package com.stefan_grafberger.streamdq.anomalydetection

/**
 * The common interface for all anomaly detection strategies
 */
interface AnomalyDetectionStrategy {
    fun detect(dataStream: List<Double>,
               searchInterval: Pair<Int, Int>)
}