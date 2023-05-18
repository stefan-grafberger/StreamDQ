package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2

class AverageAggregate
    : AggregateFunction<AggregateConstraintResult, Tuple2<Double, Long>, Double> {
    override fun createAccumulator(): Tuple2<Double, Long> {
        return Tuple2(0.0, 0L)
    }

    override fun add(value: AggregateConstraintResult, acc: Tuple2<Double, Long>): Tuple2<Double, Long> {
        return Tuple2<Double, Long>(acc.f0 + value.aggregate!!, acc.f1 + 1L)
    }

    override fun getResult(acc: Tuple2<Double, Long>): Double {
        return acc.f0 / acc.f1
    }

    override fun merge(acc0: Tuple2<Double, Long>, acc1: Tuple2<Double, Long>): Tuple2<Double, Long> {
        return Tuple2<Double, Long>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1)
    }
}