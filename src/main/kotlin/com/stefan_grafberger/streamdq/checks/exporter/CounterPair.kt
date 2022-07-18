package com.stefan_grafberger.streamdq.checks.exporter

import org.apache.flink.metrics.Counter
import org.apache.flink.metrics.Gauge

data class CounterPair(
    val successCounter: Counter,
    val failureCounter: Counter
) {
    fun increase(success: Boolean) {
        if (success) successCounter.inc()
        else failureCounter.inc()
    }
}

data class AggregateCounterTriplet(
    val successCounter: Counter,
    val failureCounter: Counter,
    val aggregateGauge: DoubleGauge
) {
    fun measure(success: Boolean, aggregateCount: Double) {
        aggregateGauge.value = aggregateCount
        if (success) successCounter.inc()
        else failureCounter.inc()
    }
}

data class DoubleGauge(var value: Double) : Gauge<Double> {
    override fun getValue() = value
}
