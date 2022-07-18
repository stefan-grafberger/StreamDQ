package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory
import kotlin.math.ceil

class HllSketchCountDistinctAggregate<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    val expectedLowerBound: Int? = null,
    val expectedUpperBound: Int? = null,
    private val checkName: String,
    val keyExpressionString: String,
    config: ExecutionConfig?
) :
    TypeQueryableAggregateFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun createAccumulator(): HllSketch {
        return HllSketch()
    }

    override fun add(input: T, hllSketchAggregation: Any): HllSketch {
        hllSketchAggregation as HllSketch
        val fieldValue = this.fieldAccessor.get(input)
        hllSketchAggregation.update(fieldValue)

        return hllSketchAggregation
    }

    override fun getResult(hllSketchAggregation: Any): AggregateConstraintResult {
        hllSketchAggregation as HllSketch
        val (_, estimate) = hllSketchAggregation.result
        val estimateInt = ceil(estimate)
        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= estimateInt) &&
            (expectedUpperBound == null || expectedUpperBound >= estimateInt)

        val output = AggregateConstraintResult(withinBounds, estimateInt, checkName)
        return output
    }

    override fun merge(hllSketchAggregation: Any, acc1: Any): HllSketch {
        hllSketchAggregation as HllSketch
        acc1 as HllSketch
        return acc1.merge(hllSketchAggregation)
    }

    override fun getIntermediateResultType(): TypeInformation<Any> {
        @Suppress("UNCHECKED_CAST")
        return TypeInformation.of(HllSketch::class.java) as TypeInformation<Any>
    }
}
