package com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import org.apache.flink.api.common.functions.MapFunction

/**
 * BoundMapFunction is used to detect anomalies
 * based on the data's value in the stream and the
 * user-defined threshold
 *
 * This aggregate function is used in
 * [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy]
 *
 * @param lowerBound Upper bound of element value in the stream
 * @param upperBound Upper bound of element value in the stream
 */
class BoundMapFunction(
        private val lowerBound: Double,
        private val upperBound: Double
) : MapFunction<AnomalyCheckResult, AnomalyCheckResult> {
    override fun map(element: AnomalyCheckResult): AnomalyCheckResult {
        return if (element.value!! !in lowerBound..upperBound) {
            AnomalyCheckResult(element.value, true, element.detail)
        } else {
            AnomalyCheckResult(element.value, false, element.detail)
        }
    }
}