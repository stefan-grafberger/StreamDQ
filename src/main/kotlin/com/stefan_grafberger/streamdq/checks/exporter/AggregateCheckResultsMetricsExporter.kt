package com.stefan_grafberger.streamdq.checks.exporter

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.ConstraintResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class AggregateCheckResultsMetricsExporter : RichMapFunction<AggregateCheckResult, AggregateCheckResult>() {
    // TODO: we should use generics here and merge both metric exporters into one

    private val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    private val metricHoldersByCheck: MutableMap<String, AggregateCounterTriplet> = mutableMapOf()

    override fun map(checkResults: AggregateCheckResult): AggregateCheckResult {
        checkResults.constraintResults
            ?.forEach {
                val pair = lookUpCounter(it)
                pair.measure(it.outcome ?: false, it.aggregate!!)
            }

        logResults()
        return checkResults
    }

    private fun logResults() {
        metricHoldersByCheck.forEach {
            LOG.debug("[${this.hashCode()}] OK: [${it.value.successCounter.count}] NOK: [${it.value.failureCounter.count}] for [${it.key}]")
        }
    }

    private fun lookUpCounter(constraintResult: ConstraintResult): AggregateCounterTriplet {
        return metricHoldersByCheck.get(constraintResult.constraintName) ?: createCounterPair(constraintResult)
    }

    private fun createCounterPair(constraintResult: ConstraintResult): AggregateCounterTriplet {
        val successCounter = runtimeContext.metricGroup.counter(constraintResult.constraintName!! + "_success")
        val failureCounter = runtimeContext.metricGroup.counter(constraintResult.constraintName!! + "_failure")
        val aggregateGauge = runtimeContext.metricGroup.gauge(constraintResult.constraintName!! + "_count", DoubleGauge(0.0))
        val triplet = AggregateCounterTriplet(successCounter, failureCounter, aggregateGauge)

        metricHoldersByCheck[constraintResult.constraintName!!] = triplet

        return triplet
    }
}
