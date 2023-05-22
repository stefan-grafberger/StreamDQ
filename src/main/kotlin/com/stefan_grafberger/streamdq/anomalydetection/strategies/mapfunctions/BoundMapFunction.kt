package com.stefan_grafberger.streamdq.anomalydetection.strategies.mapfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import org.apache.flink.api.common.functions.MapFunction

class BoundMapFunction(
        private val lowerBound: Double,
        private val upperBound: Double
) : MapFunction<AnomalyCheckResult, AnomalyCheckResult> {
    override fun map(element: AnomalyCheckResult): AnomalyCheckResult {
        return if (element.value!! !in lowerBound..upperBound) {
            AnomalyCheckResult(element.value, true, element.confidence, element.detail)
        } else {
            AnomalyCheckResult(element.value, false, element.confidence, element.detail)
        }
    }
}