package com.stefan_grafberger.streamdq.anomalydetection.model

data class AbsoluteChangeAccumulator(
        var currentValue: Double,
        var lastElementOfEachOrderList: MutableList<Double> = mutableListOf(),
        var currentChangeRate: Double,
        var count: Long
)