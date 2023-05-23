package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.anomalydetection.AnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateConstraint
import com.stefan_grafberger.streamdq.checks.aggregate.InternalAggregateCheck
import com.stefan_grafberger.streamdq.checks.row.MapFunctionsWrapper
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.datasketches.AddKeyInfo
import com.stefan_grafberger.streamdq.datasketches.KeyedAggregateFunctionsWrapper
import com.stefan_grafberger.streamdq.datasketches.KeyedFinalAggregateFunction
import com.stefan_grafberger.streamdq.datasketches.NonKeyedAggregateFunctionsWrapper
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

class AnalysisRunner {

    fun <IN> addChecksToStream(
            stream: DataStream<IN>,
            rowLevelChecksWithPotentialDuplicates: List<RowLevelCheck>,
            continuousChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
            config: ExecutionConfig?
    ): VerificationResult<IN, Any> {
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
        return VerificationResult(
                rowLevelResultMap,
                aggregateResultMap,
                rowLevelCheckIndexMap
        )
    }

    fun <IN> addChecksToStream(
            stream: DataStream<IN>,
            aggregateConstraints: MutableList<AggregateConstraint>,
            anomalyDetectionsWithPotentialDuplicates: List<AnomalyDetector>,
            rowLevelChecksWithPotentialDuplicates: List<RowLevelCheck>,
            continuousChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
            config: ExecutionConfig?
    ): VerificationResult<IN, Any> {
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

        val anomalyDetectionsResultMap = buildAndAddAnomalyDetectionResultStreams(
                stream,
                streamObjectTypeInfo,
                aggregateConstraints,
                anomalyDetectionsWithPotentialDuplicates,
                config
        )

        return VerificationResult(
                rowLevelResultMap,
                aggregateResultMap,
                rowLevelCheckIndexMap,
                anomalyDetectionsResultMap
        )
    }

    fun <IN, KEY> addChecksToStream(
            stream: KeyedStream<IN, KEY>,
            rowLevelChecksWithPotentialDuplicates: List<RowLevelCheck>,
            continuousChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
            config: ExecutionConfig?
    ): VerificationResult<IN, KEY> {
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
        return VerificationResult(
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
    ): Map<InternalAggregateCheck, DataStream<AggregateCheckResult<KEY>>> {
        val aggregateResultMap: MutableMap<InternalAggregateCheck, DataStream<AggregateCheckResult<KEY>>> = mutableMapOf()
        val uniqueContinuousAggChecks = windowChecksWithPotentialDuplicates.toSet().toList()
        // TODO: Performance optimization: use one shared stream per identical trigger/window
        //  But could also rely on user for now to specify checks accordingly
        uniqueContinuousAggChecks.forEach { check ->
            val windowedStream = check.addWindowOrTriggerKeyed(baseStream)
            val aggregateFunctions = check.constraints.map { constraint ->
                constraint.getAggregateFunction(streamObjectTypeInfo, config)
            }
            val resultStream = if (check.aggregateResultsPerKeyToGlobalResult) {
                // We need to get the state per partition per window, then combine the results of each partition to only
                //  have one global result per window
                val windowProcessingFunctionWrapper = KeyedAggregateFunctionsWrapper(aggregateFunctions)
                val processedWindowStream = windowedStream.aggregate(windowProcessingFunctionWrapper)
                val allWindowedStream = check.addWindowOrTriggerNonKeyed(processedWindowStream, true)

                val windowResultMergeFunctionWrapper = KeyedFinalAggregateFunction<IN, KEY>(aggregateFunctions)
                allWindowedStream.aggregate(windowResultMergeFunctionWrapper)
            } else {
                // We need to get the state per partition per window, and can then return the result without
                //  merging it to one global result
                val functionWrapper = NonKeyedAggregateFunctionsWrapper<IN, KEY>(aggregateFunctions)
                windowedStream.aggregate(functionWrapper, AddKeyInfo())
            }
            @Suppress("UNCHECKED_CAST")
            resultStream as DataStream<AggregateCheckResult<KEY>>
            aggregateResultMap[check] = resultStream
        }
        return aggregateResultMap
    }

    private fun <IN> buildAndAddAggResultStreams(
            baseStream: DataStream<IN>,
            windowChecksWithPotentialDuplicates: List<InternalAggregateCheck>,
            streamObjectTypeInfo: TypeInformation<IN>,
            config: ExecutionConfig?
    ): Map<InternalAggregateCheck, DataStream<AggregateCheckResult<Any>>> {
        val aggregateResultMap: MutableMap<InternalAggregateCheck, DataStream<AggregateCheckResult<Any>>> = mutableMapOf()
        val uniqueContinuousAggChecks = windowChecksWithPotentialDuplicates.toSet().toList()
        // TODO: Performance optimization: use one shared stream per identical trigger/window
        uniqueContinuousAggChecks.forEach { check ->
            val allWindowedStream = check.addWindowOrTriggerNonKeyed(baseStream, false)
            val aggregateFunctions = check.constraints.map { constraint ->
                constraint.getAggregateFunction(streamObjectTypeInfo, config)
            }
            val functionWrapper = NonKeyedAggregateFunctionsWrapper<IN, Any>(aggregateFunctions)
            val resultStream = allWindowedStream.aggregate(functionWrapper)
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

    /**
     * require firstly apply aggregate constraints functions on the stream,
     * then anomaly detection can be applied
     */
    private fun <IN> buildAndAddAnomalyDetectionResultStreams(
            baseStream: DataStream<IN>,
            streamObjectTypeInfo: TypeInformation<IN>,
            constraints: MutableList<AggregateConstraint>,
            anomalyDetectionsWithPotentialDuplicates: List<AnomalyDetector>,
            config: ExecutionConfig?
    ): Map<AnomalyDetector, DataStream<AnomalyCheckResult>> {
        val anomalyDetectionsResultMap: MutableMap<AnomalyDetector, DataStream<AnomalyCheckResult>> = mutableMapOf()
        val uniqueAnomalyDetections = anomalyDetectionsWithPotentialDuplicates.distinct()

        require(constraints.size == uniqueAnomalyDetections.size) {
            "anomaly detections must have the same length as constraints, " +
                    "since we want to do anomaly detection based on aggregate " +
                    "constraint result of aggregate constraints"
        }

        val constraintsToDetectorMap: Map<AggregateConstraint, AnomalyDetector> = constraints
                .zip(uniqueAnomalyDetections)
                .toMap()

        for ((constraint, detector) in constraintsToDetectorMap) {
            val aggregateFunction = constraint.getAggregateFunction(streamObjectTypeInfo, config)
            val resultStream = detector.detectAnomalyStream(baseStream
                    .windowAll(GlobalWindows.create())
                    .aggregate(aggregateFunction))
            anomalyDetectionsResultMap[detector] = resultStream
        }

        return anomalyDetectionsResultMap
    }
}
