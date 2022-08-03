package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.checks.AggregateCheckResult
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.Check
import com.stefan_grafberger.streamdq.checks.RowLevelCheckResult
import com.stefan_grafberger.streamdq.checks.aggregate.ContinuousAggregateCheck
import com.stefan_grafberger.streamdq.checks.aggregate.InternalAggregateCheck
import com.stefan_grafberger.streamdq.checks.aggregate.WindowAggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.data.ClickInfo
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Assertions

object TestUtils {
    const val LOCAL_PARALLELISM = 1

    fun assertExpectedAggregates(
        hllStreamOne: SingleOutputStreamOperator<AggregateConstraintResult>?,
        expectedTotalLength: Int,
        expectedAggregates: DoubleArray
    ) {
        val collectedResultOne = hllStreamOne!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(expectedTotalLength, collectedResultOne.size)

        val timestampHllResults = collectedResultOne.take(expectedAggregates.size)
            .map { constraintResult -> constraintResult.aggregate!! }
            .toDoubleArray()
        Assertions.assertArrayEquals(expectedAggregates, timestampHllResults, 0.01)
    }

    fun <IN> collectRowLevelResultStreamAndAssertLen(
        verificationResult: VerificationResult<IN, *>,
        rowLevelCheck: RowLevelCheck,
        expectedResultStreamLen: Int
    ): List<RowLevelCheckResult<IN>> {
        val checkResultStream = verificationResult.getResultsForCheck(rowLevelCheck)
        val resultList = checkResultStream!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(expectedResultStreamLen, resultList.size)
        return resultList
    }

    fun<IN> assertRowLevelConstraintResults(
        result: List<RowLevelCheckResult<IN>>,
        rowLevelCheck: RowLevelCheck,
        constraintIndex: Int,
        expectedOutcomes: Array<Boolean>
    ) {
        val (constraintName, outcomeArray) = getRowLevelConstraintResultsByIndex(result, constraintIndex)
        Assertions.assertEquals(constraintName, getExpectedRowLevelConstraintNameByIndex(rowLevelCheck, constraintIndex))
        val verifyFirstNElements = expectedOutcomes.size
        val limitedOutcomeList = outcomeArray.take(verifyFirstNElements).toTypedArray()
        Assertions.assertArrayEquals(expectedOutcomes, limitedOutcomeList)
    }

    private fun getExpectedAggregateConstraintNameByIndex(
        aggregateCheck: InternalAggregateCheck,
        constraintIndex: Int
    ) = aggregateCheck.constraints[constraintIndex].toString()

    private fun getExpectedRowLevelConstraintNameByIndex(
        rowLevelCheck: RowLevelCheck,
        constraintIndex: Int
    ) = rowLevelCheck.constraints[constraintIndex].toString()

    private fun getAggregateConstraintResultsByIndex(
        collectedResultOne: List<AggregateCheckResult<*>>,
        constraintIndex: Int
    ): Triple<String?, Array<Double>, Array<Boolean>> {
        val constraintName = collectedResultOne[constraintIndex].constraintResults!![constraintIndex].constraintName
        val aggregateNumArray = collectedResultOne
            .map { aggCheckResult -> aggCheckResult.constraintResults!![constraintIndex].aggregate!! }
            .toTypedArray()
        val outcomeArray =
            collectedResultOne
                .map { aggregateCheckResult -> aggregateCheckResult.constraintResults!![constraintIndex].outcome!! }
                .toTypedArray()
        return Triple(constraintName, aggregateNumArray, outcomeArray)
    }

    private fun<IN> getRowLevelConstraintResultsByIndex(
        collectedResultOne: List<RowLevelCheckResult<IN>>,
        constraintIndex: Int
    ): Pair<String?, Array<Boolean>> {
        val constraintName = collectedResultOne[constraintIndex].constraintResults!![constraintIndex].constraintName
        val outcomeArray =
            collectedResultOne
                .map { rowLevelCheckResult -> rowLevelCheckResult.constraintResults!![constraintIndex].outcome!! }
                .toTypedArray()
        return Pair(constraintName, outcomeArray)
    }

    fun<T> assertFailsWithExpectedMessage(
        rawStream: SingleOutputStreamOperator<T>,
        env: StreamExecutionEnvironment,
        invalidFieldCheck: Check,
        expectedExceptionMessage: String
    ) {
        val exception = Assertions.assertThrows(CompositeType.InvalidFieldReferenceException::class.java) {
            val builder = VerificationSuite().onDataStream(rawStream, env.config)
            when (invalidFieldCheck) {
                is InternalAggregateCheck -> builder.addAggregateCheck(invalidFieldCheck)
                is RowLevelCheck -> builder.addRowLevelCheck(invalidFieldCheck)
                else -> throw java.lang.IllegalStateException("The are only two check types!")
            }
            builder.build()
        }
        Assertions.assertEquals(expectedExceptionMessage, exception.message)
    }

    fun<KEY> collectAggregateResultStreamAndAssertLen(
        verificationResult: VerificationResult<ClickInfo, KEY>,
        aggregateCheck: InternalAggregateCheck,
        expectedResultStreamLen: Int
    ): List<AggregateCheckResult<KEY>> {
        val resultStream = when (aggregateCheck) {
            is ContinuousAggregateCheck<*> -> verificationResult.getResultsForCheck(aggregateCheck)
            is WindowAggregateCheck<*> -> verificationResult.getResultsForCheck(aggregateCheck)
            else -> throw IllegalStateException("$aggregateCheck is not a known InternalAggregateCheck type!")
        }
        val collectedResult = resultStream!!.executeAndCollect().asSequence().toList()
        Assertions.assertEquals(expectedResultStreamLen, collectedResult.size)
        return collectedResult
    }

    fun assertAggregateConstraintResults(
        result: List<AggregateCheckResult<*>>,
        constraintIndex: Int,
        aggregateCheck: InternalAggregateCheck,
        expectedMetrics: DoubleArray,
        expectedOutcomes: Array<Boolean>
    ) {
        val (constraintName, aggregateNumArray, outcomeArray) = getAggregateConstraintResultsByIndex(result, constraintIndex)
        Assertions.assertEquals(constraintName, getExpectedAggregateConstraintNameByIndex(aggregateCheck, constraintIndex))
        Assertions.assertArrayEquals(expectedMetrics, aggregateNumArray.toDoubleArray(), 0.01)
        Assertions.assertArrayEquals(expectedOutcomes, outcomeArray)
    }

    fun<KEY> assertAggregateConstraintResultsWithNameAsString(
        result: List<AggregateCheckResult<KEY>>,
        constraintIndex: Int,
        aggregateCheckName: String,
        expectedMetrics: DoubleArray,
        expectedOutcomes: Array<Boolean>
    ) {
        val (constraintName, aggregateNumArray, outcomeArray) = getAggregateConstraintResultsByIndex(result, constraintIndex)
        Assertions.assertEquals(constraintName, aggregateCheckName)
        Assertions.assertArrayEquals(expectedMetrics, aggregateNumArray.toDoubleArray(), 0.01)
        Assertions.assertArrayEquals(expectedOutcomes, outcomeArray)
    }
}
