package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.anomalydetection.detectors.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import com.stefan_grafberger.streamdq.checks.aggregate.InternalAggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream

data class VerificationResult<IN, KEY>(
        private val rowLevelCheckResults: Map<RowLevelCheck, DataStream<RowLevelCheckResult<IN>>>,
        private val aggregateCheckResults: Map<InternalAggregateCheck, DataStream<AggregateCheckResult<KEY>>>,
        private val rowLevelChecksWithIndex: Map<RowLevelCheck, Int>,
        private val anomalyDetectionsResults: Map<AnomalyDetector, DataStream<AnomalyCheckResult>>? = null
) {

    fun getResultsForCheck(check: InternalAggregateCheck): DataStream<AggregateCheckResult<KEY>>? {
        return aggregateCheckResults[check]
    }

    fun getResultsForCheck(check: RowLevelCheck): DataStream<RowLevelCheckResult<IN>>? { // Can we make this into a non-nullable object and throw an Exception when we can't retrieve the results for check?
        return rowLevelCheckResults[check]
    }

    fun getResultsForCheck(detector: AnomalyDetector): DataStream<AnomalyCheckResult>? {
        return anomalyDetectionsResults?.get(detector)
    }
}

class VerificationSuite {
    fun <IN> onDataStream(stream: DataStream<IN>, config: ExecutionConfig?): VerificationPipelineBuilder<DataStream<IN>, IN, Any> {
        return VerificationPipelineBuilder(stream, config)
    }

    fun <IN, KEY> onDataStream(stream: KeyedStream<IN, KEY>, config: ExecutionConfig?): VerificationPipelineBuilder<DataStream<IN>, IN, KEY> {
        return VerificationPipelineBuilder(stream, config)
    }
}

class VerificationPipelineBuilder<STYPE, IN, KEY>(val stream: STYPE, val config: ExecutionConfig?) {
    var rowLevelChecks = mutableListOf<RowLevelCheck>()
    var aggChecks = mutableListOf<InternalAggregateCheck>()
    var anomalyChecks = mutableListOf<AnomalyDetector>()
    var aggregateConstraints = mutableListOf<AggregateConstraint>()

    fun addRowLevelCheck(newRowLevelCheck: RowLevelCheck): VerificationPipelineBuilder<STYPE, IN, KEY> {
        rowLevelChecks.add(newRowLevelCheck)
        return this
    }

    fun addRowLevelChecks(newRowLevelChecks: Collection<RowLevelCheck>): VerificationPipelineBuilder<STYPE, IN, KEY> {
        rowLevelChecks.addAll(newRowLevelChecks)
        return this
    }

    fun addAggregateCheck(newAggCheck: InternalAggregateCheck): VerificationPipelineBuilder<STYPE, IN, KEY> {
        aggChecks.add(newAggCheck)
        return this
    }

    fun addAggregateChecks(newAggChecks: Collection<InternalAggregateCheck>): VerificationPipelineBuilder<STYPE, IN, KEY> {
        aggChecks.addAll(newAggChecks)
        return this
    }

    /**
     * able to add only one anomaly checks
     */
    fun addAnomalyCheck(newAnomalyCheck: AnomalyDetector): VerificationPipelineBuilder<STYPE, IN, KEY> {
        anomalyChecks.add(newAnomalyCheck)
        return this
    }

    fun addAnomalyChecks(newAnomalyCheck: Collection<AnomalyDetector>): VerificationPipelineBuilder<STYPE, IN, KEY> {
        anomalyChecks.addAll(newAnomalyCheck)
        return this
    }

    fun addAggregateConstraint(constraint: AggregateConstraint): VerificationPipelineBuilder<STYPE, IN, KEY> {
        aggregateConstraints.add(constraint)
        return this
    }

    fun addAggregateConstraints(constraint: Collection<AggregateConstraint>): VerificationPipelineBuilder<STYPE, IN, KEY> {
        aggregateConstraints.addAll(constraint)
        return this
    }

    fun build(): VerificationResult<IN, *> {
        return when (stream) {
            is KeyedStream<*, *> -> {
                @Suppress("UNCHECKED_CAST")
                val typedStream = stream as KeyedStream<IN, KEY>
                AnalysisRunner()
                        .addChecksToStream(typedStream, rowLevelChecks, aggChecks, config)
            }

            is DataStream<*> -> {
                @Suppress("UNCHECKED_CAST")
                val typedStream = stream as DataStream<IN>
                AnalysisRunner()
                        .addChecksToStream(typedStream, aggregateConstraints, anomalyChecks, rowLevelChecks, aggChecks, config)
            }

            else -> {
                throw IllegalStateException("This should never happen!")
            }
        }
    }
}
