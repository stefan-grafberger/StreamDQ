package com.stefan_grafberger.streamdq.anomalydetection.model

data class AnomalyCheckResult(
        val value: Double?,
        val isAnomaly: Boolean? = null,
        val confidence: Double,
        val detail: String? = null
)