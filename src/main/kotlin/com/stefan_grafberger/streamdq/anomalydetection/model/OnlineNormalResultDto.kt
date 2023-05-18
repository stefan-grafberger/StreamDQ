package com.stefan_grafberger.streamdq.anomalydetection.model

data class OnlineNormalResultDto(val value: Double = 0.0,
                                 val mean: Double = 0.0,
                                 val stdDev: Double = 0.0,
                                 val isAnomaly: Boolean = false)