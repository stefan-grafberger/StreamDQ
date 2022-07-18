package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.aggregate.InternalAggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream

data class VerificationResult<IN>(
    private val rowLevelCheckResults: Map<RowLevelCheck, DataStream<RowLevelCheckResult<IN>>>,
    private val aggregateCheckResults: Map<InternalAggregateCheck, DataStream<AggregateCheckResult>>,
    private val rowLevelChecksWithIndex: Map<RowLevelCheck, Int>
) {

    fun getResultsForCheck(check: InternalAggregateCheck): DataStream<AggregateCheckResult>? {
        return aggregateCheckResults[check]
    }

    fun getResultsForCheck(check: RowLevelCheck): DataStream<RowLevelCheckResult<IN>>? { // Can we make this into a non-nullable object and throw an Exception when we can't retrieve the results for check?
        return rowLevelCheckResults[check]
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

    fun build(): VerificationResult<IN> {
        return when (stream) {
            is KeyedStream<*, *> -> {
                @Suppress("UNCHECKED_CAST")
                val typedStream = stream as KeyedStream<IN, KEY>
                com.stefan_grafberger.streamdq.AnalysisRunner()
                    .addChecksToStream(typedStream, rowLevelChecks, aggChecks, config)
            }
            is DataStream<*> -> {
                @Suppress("UNCHECKED_CAST")
                val typedStream = stream as DataStream<IN>
                com.stefan_grafberger.streamdq.AnalysisRunner()
                    .addChecksToStream(typedStream, rowLevelChecks, aggChecks, config)
            }
            else -> {
                throw IllegalStateException("This should never happen!")
            }
        }
    }
}
