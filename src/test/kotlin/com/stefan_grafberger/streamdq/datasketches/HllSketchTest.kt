package com.stefan_grafberger.streamdq.datasketches

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.math.BigInteger

class HllSketchTest {
    @Test
    fun `test update`() {
        val aggregation = HllSketch()
        aggregation.update("A")
        aggregation.update("A")
        aggregation.update("A")
        aggregation.update("B")
        aggregation.update("B")
        aggregation.update("C")
        aggregation.update("D")
        assertEquals(BigInteger.valueOf(7), aggregation.totalCount)
        assertEquals(4, aggregation.sketch.estimate.toInt())
    }

    @Test
    fun `test merge`() {
        val aggregation = HllSketch()
        aggregation.update(1)
        aggregation.update(2)
        aggregation.update(3)
        aggregation.update(1)
        aggregation.update(2)

        val aggregation2 = HllSketch()
        aggregation2.update(3)
        aggregation2.update(4)
        aggregation2.update(5)
        aggregation2.update(2)
        aggregation2.update(3)

        val mergedAggregation = aggregation.merge(aggregation2)
        assertEquals(BigInteger.valueOf(10), mergedAggregation.totalCount)
        assertEquals(5, mergedAggregation.sketch.estimate.toInt())
    }

    @Test
    fun `test result`() {
        val aggregation = HllSketch()
        aggregation.update(1)
        aggregation.update(2)
        aggregation.update(3)
        aggregation.update(2)
        aggregation.update(3)
        val estimateResult: Pair<BigInteger, Double> = aggregation.result
        val expectedResult: Pair<BigInteger, Double> = Pair(BigInteger.valueOf(5), 3.0)
        assertEquals(estimateResult.first, expectedResult.first)
        assertEquals(estimateResult.second, expectedResult.second, 0.01)
    }

    @Test
    fun `test updateSketch`() {
        val aggregation = HllSketch()
        aggregation.update(1)
        aggregation.update(2.toLong())
        aggregation.update(3.toByte())
        aggregation.update("b")
        aggregation.update("string")

        aggregation.update(intArrayOf(4))
        aggregation.update(charArrayOf('a'))

        // Duplicates
        aggregation.update(longArrayOf(2))
        aggregation.update(byteArrayOf(3))
        aggregation.update(null)

        assertEquals(BigInteger.valueOf(10), aggregation.totalCount)
        assertEquals(7, aggregation.sketch.estimate.toInt())

        val exception = assertThrows(IllegalArgumentException::class.java) {
            aggregation.update(Object())
        }
        assertEquals("Type is not support currently: \"java.lang.Object\"", exception.message)
    }
}
