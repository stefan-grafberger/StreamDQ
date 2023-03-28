package com.stefan_grafberger.streamdq.anomalydetection.model

data class OnlineNormalResultDto(val mean: Double,
                                 val stdDev: Double,
                                 val isAnomaly: Boolean)