package com.stefan_grafberger.streamdq.datasketches

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class KllSketchTest {
    @Test
    fun `test update`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1.toDouble())
        aggregation.update(4)
        aggregation.update(20)
        aggregation.update(80)
        aggregation.update(81.0)
        aggregation.update(85)
        aggregation.update(90.0)
        assertEquals(80.0, aggregation.result, 0.01)
    }

    @Test
    fun `test merge`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1.toDouble())
        aggregation.update(4)
        aggregation.update(20)
        aggregation.update(80)
        aggregation.update(81.0)

        val aggregation2 = KllSketch(0.5)
        aggregation.update(85)
        aggregation.update(90.0)

        val mergedAggregation = aggregation.merge(aggregation2)
        assertEquals(80.0, mergedAggregation.result, 0.01)
    }

    @Test
    fun `test updateSketch`() {
        val aggregation = KllSketch(0.5)
        aggregation.update(1)
        aggregation.update(2.toLong())
        aggregation.update(3.toByte())
        aggregation.update("10")
        aggregation.update("40")

        assertEquals(3.0, aggregation.result, 0.01)

        val exception = assertThrows(IllegalArgumentException::class.java) {
            aggregation.update(Object())
        }
        assertEquals("Type is not support currently: \"java.lang.Object\"", exception.message)
    }
}
