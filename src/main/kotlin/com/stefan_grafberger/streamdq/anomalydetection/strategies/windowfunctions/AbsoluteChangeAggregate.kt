/**
 * Licensed to the University of Amsterdam (UvA) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The UvA licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stefan_grafberger.streamdq.anomalydetection.strategies.windowfunctions

import com.stefan_grafberger.streamdq.anomalydetection.model.accumulator.AbsoluteChangeAccumulator
import com.stefan_grafberger.streamdq.anomalydetection.model.result.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * AbsoluteChangeAggregate function is used to detect anomalies
 * based on the data's absolute change(minus) in the stream.
 * Taking an embedded list in the accumulator approach. The list
 * has a length of order and stores its computation result of each
 * order and will be updated when the next new element comes.
 *
 * This aggregate function is used in
 * [com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.AbsoluteChangeStrategy]
 *
 * @param maxRateDecrease Upper bound of accepted decrease (lower bound of increase).
 * @param maxRateIncrease Upper bound of accepted growth.
 * @param order           order of the derivative
 */
class AbsoluteChangeAggregate(
        private val maxRateDecrease: Double = -Double.MAX_VALUE,
        private val maxRateIncrease: Double = Double.MAX_VALUE,
        private val order: Int = 1
) : AggregateFunction<AggregateConstraintResult,
        AbsoluteChangeAccumulator,
        AnomalyCheckResult> {

    private var currentValue = 0.0
    private var preOrderList: MutableList<Double> = mutableListOf()
    private var initialList: MutableList<Double> = mutableListOf()

    /**
     * accumulator (currentValue, list of last element of each order, currentChangeRate, count)
     * the list contains the last value of Order n(order integer) to order 1
     * the first order elements will be none anomaly since we can not determine
     */
    override fun createAccumulator(): AbsoluteChangeAccumulator {
        return AbsoluteChangeAccumulator(0.0, mutableListOf(), 0.0, 0L)
    }

    override fun add(
            aggregateConstraintResult: AggregateConstraintResult,
            acc: AbsoluteChangeAccumulator
    )
            : AbsoluteChangeAccumulator {

        currentValue = aggregateConstraintResult.aggregate!!
        if (acc.count < order) {
            preOrderList.add(currentValue)
        } else {
            if (acc.count?.toInt() == order) acc.lastElementOfEachOrderList.addAll(initializeBeforeDetect(preOrderList, order))
            val lastElementOfEachOrderList: MutableList<Double> = mutableListOf()
            var currentOrderChange = currentValue
            for (i in 0 until order) {
                lastElementOfEachOrderList.add(currentOrderChange)
                currentOrderChange -= acc.lastElementOfEachOrderList.elementAt(i)
            }
            acc.currentChangeRate = lastElementOfEachOrderList.last() - acc.lastElementOfEachOrderList.last()
            acc.lastElementOfEachOrderList = lastElementOfEachOrderList
        }
        acc.currentValue = currentValue
        acc.count += 1L
        return AbsoluteChangeAccumulator(acc.currentValue, acc.lastElementOfEachOrderList, acc.currentChangeRate, acc.count)
    }

    override fun getResult(acc: AbsoluteChangeAccumulator): AnomalyCheckResult {
        val currentChangeRate = acc.currentChangeRate
        if (acc.count > order && currentChangeRate !in maxRateDecrease..maxRateIncrease) {
            return AnomalyCheckResult(acc.currentValue, true)
        }
        return AnomalyCheckResult(acc.currentValue, false)
    }

    override fun merge(
            acc0: AbsoluteChangeAccumulator,
            acc1: AbsoluteChangeAccumulator
    ): AbsoluteChangeAccumulator {
        return AbsoluteChangeAccumulator(
                acc0.currentValue + acc1.currentValue,
                (acc0.lastElementOfEachOrderList + acc1.lastElementOfEachOrderList).toMutableList(),
                acc0.currentChangeRate + acc1.currentChangeRate,
                acc0.count + acc1.count
        )
    }

    private fun initializeBeforeDetect(
            preOrderList: MutableList<Double>,
            order: Int
    ): MutableList<Double> {
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
