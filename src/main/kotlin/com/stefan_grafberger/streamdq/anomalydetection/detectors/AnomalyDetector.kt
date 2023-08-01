package com.stefan_grafberger.streamdq.anomalydetection.detectors

import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

/**
 * The interface for AnomalyDetector which is used
 * for windowing the data stream source at the beginning
 * and perform aggregate computation of data quality constraints
 * if needed
 *
 * @Override by [com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector]
 */
interface AnomalyDetector {
    fun <IN> detectAnomalyStream(dataStream: DataStream<IN>): SingleOutputStreamOperator<AnomalyCheckResult>
}
