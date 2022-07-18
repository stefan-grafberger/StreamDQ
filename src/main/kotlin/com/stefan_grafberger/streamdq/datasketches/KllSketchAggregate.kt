package com.stefan_grafberger.streamdq.datasketches

import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.TypeQueryableAggregateFunction
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessor
import org.apache.flink.streaming.util.typeutils.NullCheckingFieldAccessorFactory

class KllSketchAggregate<T>(
    streamObjectTypeInfo: TypeInformation<T>,
    private val quantile: Double,
    val expectedLowerBound: Double? = null,
    val expectedUpperBound: Double? = null,
    private val checkName: String,
    val keyExpressionString: String,
    config: ExecutionConfig?
) :
    TypeQueryableAggregateFunction<T>() {

    private val fieldAccessor: FieldAccessor<T, Any>
    init {
        fieldAccessor = NullCheckingFieldAccessorFactory.getAccessor(streamObjectTypeInfo, keyExpressionString, config)
    }

    override fun createAccumulator(): KllSketch {
        return KllSketch(this.quantile)
    }

    override fun add(input: T, kllSketchAggregation: Any): KllSketch {
        kllSketchAggregation as KllSketch
        val fieldValue = this.fieldAccessor.get(input)
        kllSketchAggregation.update(fieldValue)
        return kllSketchAggregation
    }

    override fun getResult(kllSketchAggregation: Any): AggregateConstraintResult {
        kllSketchAggregation as KllSketch
        val quantileValue = kllSketchAggregation.result
        val withinBounds = (expectedLowerBound == null || expectedLowerBound <= quantileValue) &&
            (expectedUpperBound == null || expectedUpperBound >= quantileValue)
        // TODO: Float comparison and epsilon
        return AggregateConstraintResult(withinBounds, quantileValue, checkName)
    }

    override fun merge(hllSketchAggregation: Any, acc1: Any): KllSketch {
        hllSketchAggregation as KllSketch
        acc1 as KllSketch
        return acc1.merge(hllSketchAggregation)
    }

    override fun getIntermediateResultType(): TypeInformation<Any> {
        @Suppress("UNCHECKED_CAST")
        return TypeInformation.of(KllSketch::class.java) as TypeInformation<Any>
    }
}
