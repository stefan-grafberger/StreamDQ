package com.stefan_grafberger.streamdq.anomalydetection.model.result

data class NormalStrategyResult(val value: Double = 0.0,
                                val mean: Double = 0.0,
                                val stdDev: Double = 0.0,
                                val isAnomaly: Boolean = false)