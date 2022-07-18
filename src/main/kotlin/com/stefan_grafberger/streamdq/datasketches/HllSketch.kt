package com.stefan_grafberger.streamdq.datasketches

import org.apache.datasketches.hll.TgtHllType
import org.apache.datasketches.hll.Union
import java.io.Serializable
import java.math.BigInteger
import org.apache.datasketches.hll.HllSketch as DataSketchHllSketch

class HllSketch : Serializable {
    var tgtHllType = TgtHllType.HLL_4
    private var lgk = 4
    var sketch: DataSketchHllSketch = DataSketchHllSketch(lgk)
    var totalCount: BigInteger = BigInteger.ZERO

    fun update(value: Any?) {
        this.totalCount += BigInteger.ONE
        when (value) {
            is Int -> { sketch.update(value.toLong()) }
            is Byte -> { sketch.update(byteArrayOf(value)) }
            is Char -> { sketch.update(charArrayOf(value)) }
            is Long -> { sketch.update(longArrayOf(value)) }
            is Double -> { sketch.update(value) }
            is LongArray -> { sketch.update(value) }
            is IntArray -> { sketch.update(value) }
            is CharArray -> { sketch.update(value) }
            is ByteArray -> { sketch.update(value) }
            is String -> { sketch.update(value) }
            null -> {}
            else -> {
                throw IllegalArgumentException("Type is not support currently: \"${value.javaClass.name}\"")
            }
        }
    }

    override fun toString(): String {
        return """HllSketchAggregation(${this.totalCount}, sketch=${this.sketch.estimate})"""
    }

    val result: Pair<BigInteger, Double>
        get() {
            return Pair(this.totalCount, this.sketch.estimate)
        }

    fun merge(edgeValue: HllSketch): HllSketch {
        val union = Union(lgk)
        union.update(edgeValue.sketch)
        union.update(this.sketch)
        this.sketch = union.result
        val newTotalCount = this.totalCount + edgeValue.totalCount
        this.totalCount = newTotalCount
        return this
    }
}
