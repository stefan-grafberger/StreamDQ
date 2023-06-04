package com.stefan_grafberger.streamdq.experiment.watermarkgenerator

import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.common.eventtime.WatermarkGenerator
import org.apache.flink.api.common.eventtime.WatermarkOutput

class TimeLagWatermarkGenerator<T> : WatermarkGenerator<T> {

    private val maxTimeLag: Long = 500 // 0.5 seconds

    override fun onEvent(p0: T?, p1: Long, p2: WatermarkOutput?) {}

    override fun onPeriodicEmit(output: WatermarkOutput?) {
        output?.emitWatermark(Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
