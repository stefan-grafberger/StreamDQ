package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple4

/**
 * accumulator (currentValue, list of last element of each order, currentChangeRate, count)
 * the list contains the last value of Order n(order integer) to order 1
 * the first order elements will be none anomaly since we can not determine
 */
class AbsoluteChangeAggregate(
        private val maxRateDecrease: Double = -Double.MIN_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1
) : AggregateFunction<AggregateConstraintResult,
        Tuple4<Double, MutableList<Double>, Double, Long>,
        AnomalyCheckResult> {

    private var currentValue = 0.0
    private var preOrderList: MutableList<Double> = mutableListOf()
    private var initialList: MutableList<Double> = mutableListOf()

    override fun createAccumulator(): Tuple4<Double, MutableList<Double>, Double, Long> {
        return Tuple4(0.0, mutableListOf(), 0.0, 0L)
    }

    override fun add(aggregateConstraintResult: AggregateConstraintResult,
                     acc: Tuple4<Double, MutableList<Double>, Double, Long>)
            : Tuple4<Double, MutableList<Double>, Double, Long> {

        currentValue = aggregateConstraintResult.aggregate!!
        if (acc.f3 < order) {
            preOrderList.add(currentValue)
        } else {
            if (acc.f3?.toInt() == order) acc.f1.addAll(initializeBeforeDetect(preOrderList, order))
            val lastElementOfEachOrderList: MutableList<Double> = mutableListOf()
            var currentOrderChange = currentValue
            for (i in 0 until order) {
                lastElementOfEachOrderList.add(currentOrderChange)
                currentOrderChange -= acc.f1.elementAt(i)
            }
            acc.f2 = lastElementOfEachOrderList.last() - acc.f1.last()
            acc.f1 = lastElementOfEachOrderList
        }
        acc.f0 = currentValue
        acc.f3 += 1L
        return Tuple4(acc.f0, acc.f1, acc.f2, acc.f3)
    }

    override fun getResult(acc: Tuple4<Double, MutableList<Double>, Double, Long>): AnomalyCheckResult {
        val currentChangeRate = acc.f2
        if (acc.f3 > order && currentChangeRate !in maxRateDecrease..maxRateIncrease) {
            return AnomalyCheckResult(acc.f0, true, 1.0)
        }
        return AnomalyCheckResult(acc.f0, false, 1.0)
    }

    override fun merge(acc0: Tuple4<Double, MutableList<Double>, Double, Long>, acc1: Tuple4<Double, MutableList<Double>, Double, Long>): Tuple4<Double, MutableList<Double>, Double, Long> {
        return Tuple4(acc0.f0 + acc1.f0, (acc0.f1 + acc1.f1).toMutableList(), acc0.f2 + acc1.f2, acc0.f3 + acc1.f3)
    }

    private fun initializeBeforeDetect(preOrderList: MutableList<Double>, order: Int): MutableList<Double> {
        require(preOrderList.size == order) { "require preOrderList has size of order to initialize before detect" }
        if (order == 1 || preOrderList.size == 0) {
            initialList.add(preOrderList.last())
        } else {
            initialList.add(preOrderList.last())
            val valuesRight = preOrderList.slice(1 until preOrderList.size)
            val valuesLeft = preOrderList.slice(0 until preOrderList.size - 1)
            initializeBeforeDetect(valuesRight
                    .zip(valuesLeft)
                    .map { (val1, val2) -> val1 - val2 }
                    .toMutableList(), order - 1)
        }
        return initialList
    }
}
