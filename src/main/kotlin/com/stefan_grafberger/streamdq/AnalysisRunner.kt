package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.aggregate.InternalAggregateCheck
import com.stefan_grafberger.streamdq.checks.row.MapFunctionsWrapper
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.datasketches.KeyedAggregateFunctionsWrapper
import com.stefan_grafberger.streamdq.datasketches.KeyedFinalAggregateFunction
import com.stefan_grafberger.streamdq.datasketches.NonKeyedAggregateFunctionsWrapper
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream

class AnalysisRunner {

    fun <IN> addChecksToStream(
        stream: DataStream<IN>,
        rowLevelChecksWithPotentialDuplicates: List<RowLevelCheck>,
        continuousChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
        config: ExecutionConfig?
    ): com.stefan_grafberger.streamdq.VerificationResult<IN> {
        val uniqueRowLevelChecks = rowLevelChecksWithPotentialDuplicates.toSet().toList()
        val streamObjectTypeInfo: TypeInformation<IN> = stream.type

        val rowLevelResultMap = buildRowLevelResultMap(stream, uniqueRowLevelChecks, streamObjectTypeInfo, config)

        val aggregateResultMap = buildAndAddAggResultStreams(
            stream,
            continuousChecksWithPotentialDuplicates,
            streamObjectTypeInfo,
            config
        )

        val rowLevelCheckIndexMap = uniqueRowLevelChecks.mapIndexed { index, check -> check to (index + 1) }.toMap()
        return com.stefan_grafberger.streamdq.VerificationResult(
            rowLevelResultMap,
            aggregateResultMap,
            rowLevelCheckIndexMap
        )
    }

    fun <IN, KEY> addChecksToStream(
        stream: KeyedStream<IN, KEY>,
        rowLevelChecksWithPotentialDuplicates: List<RowLevelCheck>,
        continuousChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
        config: ExecutionConfig?
    ): com.stefan_grafberger.streamdq.VerificationResult<IN> {
        val uniqueRowLevelChecks = rowLevelChecksWithPotentialDuplicates.toSet().toList()
        val streamObjectTypeInfo: TypeInformation<IN> = stream.type

        val rowLevelResultMap = buildRowLevelResultMap(stream, uniqueRowLevelChecks, streamObjectTypeInfo, config)

        val aggregateResultMap = buildAndAddAggResultStreams(
            stream,
            continuousChecksWithPotentialDuplicates,
            streamObjectTypeInfo,
            config
        )

        val rowLevelCheckIndexMap = uniqueRowLevelChecks.mapIndexed { index, check -> check to (index + 1) }.toMap()
        return com.stefan_grafberger.streamdq.VerificationResult(
            rowLevelResultMap,
            aggregateResultMap,
            rowLevelCheckIndexMap
        )
    }

    private fun <IN, KEY> buildAndAddAggResultStreams(
        baseStream: KeyedStream<IN, KEY>,
        windowChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
        streamObjectTypeInfo: TypeInformation<IN>,
        config: ExecutionConfig?
    ): Map<InternalAggregateCheck, DataStream<AggregateCheckResult>> {
        val aggregateResultMap: MutableMap<InternalAggregateCheck, DataStream<AggregateCheckResult>> = mutableMapOf()
        val uniqueContinuousAggChecks = windowChecksWithPotentialDuplicates.toSet().toList()
        // TODO: Performance optimization: use one shared stream per identical trigger/window
        //  But could also rely on user for now to specify checks accordingly
        uniqueContinuousAggChecks.forEach { check ->
            val windowedStream = check.addWindowOrTriggerKeyed(baseStream)
            val aggregateFunctions = check.constraints.map { constraint ->
                constraint.getAggregateFunction(streamObjectTypeInfo, config)
            }
            val windowProcessingFunctionWrapper = KeyedAggregateFunctionsWrapper(aggregateFunctions)
            // TODO: This aggregate call here is the problem for the KeyedStream. We need to get state per partition
            //  per window, then combine the results of each partition to only have one global result per window
            //  Maybe leave it as an aggregate function, but use an emtpy getResult function that outputs the state
            //  And then we can have a seperate windowAll followed by the correct trigger to combine the
            //  state results and then do the original getResult. Can use inheritance to not have too much code
            //  duplication
            val processedWindowStream = windowedStream.aggregate(windowProcessingFunctionWrapper)
            val allWindowedStream = check.addWindowOrTriggerNonKeyed(processedWindowStream, true)
            val windowResultMergeFunctionWrapper = KeyedFinalAggregateFunction(aggregateFunctions)
            val resultStream = allWindowedStream.aggregate(windowResultMergeFunctionWrapper)
            aggregateResultMap[check] = resultStream
        }
        return aggregateResultMap
    }

    private fun <IN> buildAndAddAggResultStreams(
        baseStream: DataStream<IN>,
        windowChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
        streamObjectTypeInfo: TypeInformation<IN>,
        config: ExecutionConfig?
    ): Map<InternalAggregateCheck, DataStream<AggregateCheckResult>> {
        val aggregateResultMap: MutableMap<InternalAggregateCheck, DataStream<AggregateCheckResult>> = mutableMapOf()
        val uniqueContinuousAggChecks = windowChecksWithPotentialDuplicates.toSet().toList()
        // TODO: Performance optimization: use one shared stream per identical trigger/window
        uniqueContinuousAggChecks.forEach { check ->
            val windowedStream = check.addWindowOrTriggerNonKeyed(baseStream, false)
            val aggregateFunctions = check.constraints.map { constraint ->
                constraint.getAggregateFunction(streamObjectTypeInfo, config)
            }
            val functionWrapper = NonKeyedAggregateFunctionsWrapper(aggregateFunctions)
            val resultStream = windowedStream.aggregate(functionWrapper)
            aggregateResultMap[check] = resultStream
        }
        return aggregateResultMap
    }

    private fun <IN> buildRowLevelResultMap(
        baseStream: DataStream<IN>,
        uniqueRowLevelChecks: List<RowLevelCheck>,
        streamObjectTypeInfo: TypeInformation<IN>,
        config: ExecutionConfig?
    ): Map<RowLevelCheck, DataStream<RowLevelCheckResult<IN>>> {
        val rowLevelResultMap: MutableMap<RowLevelCheck, DataStream<RowLevelCheckResult<IN>>> = mutableMapOf()
        for (rowLevelCheck in uniqueRowLevelChecks) {
            val mapFunctions = rowLevelCheck.constraints.map { constraint ->
                constraint.getCheckResultMapper(streamObjectTypeInfo, config)
            }
            val functionWrapper = MapFunctionsWrapper(mapFunctions)
            val rowLevelCheckResultStream = baseStream.map(functionWrapper)
            rowLevelResultMap[rowLevelCheck] = rowLevelCheckResultStream
        }

        return rowLevelResultMap
    }
}
