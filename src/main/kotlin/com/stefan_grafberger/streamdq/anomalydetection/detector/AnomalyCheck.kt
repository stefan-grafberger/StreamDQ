package com.stefan_grafberger.streamdq.anomalydetection.detector

import com.stefan_grafberger.streamdq.checks.Check
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window

/**
 * TODO: windowing function to be designed and added,
 * TODO: need to ask stefan for some suggestions
 */
class AnomalyCheck : Check() {
    override fun equals(other: Any?): Boolean {
        TODO("Not yet implemented")
    }

    override fun hashCode(): Int {
        TODO("Not yet implemented")
    }

    fun <W : Window> onWindow(aggregateWindowAssigner: WindowAssigner<Any?, W>) {
    }
}