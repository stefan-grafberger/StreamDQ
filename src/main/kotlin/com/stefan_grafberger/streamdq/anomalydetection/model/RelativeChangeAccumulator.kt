package com.stefan_grafberger.streamdq.anomalydetection.model

data class RelativeChangeAccumulator(
        var currentValue: Double,
        var deque: ArrayDeque<Double> = ArrayDeque(),
        var currentChangeRate: Double,
        var count: Long
)