package com.stefan_grafberger.streamdq.anomalydetection.model

data class NormalStrategyAccumulator(
        var lastMean: Double,
        var currentMean: Double,
        var lastVariance: Double,
        var currentVariance: Double,
        var lastSn: Double,
        var sn: Double,
        var count: Long
)