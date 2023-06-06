package com.stefan_grafberger.streamdq

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyDetector
import com.stefan_grafberger.streamdq.anomalydetection.model.AnomalyCheckResult
import com.stefan_grafberger.streamdq.anomalydetection.strategies.AnomalyDetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.anomalydetection.strategies.impl.SimpleThresholdStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.data.ClickType
import com.stefan_grafberger.streamdq.data.TestDataUtils
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import java.math.BigDecimal
import java.util.regex.Pattern
import kotlin.test.assertEquals
import org.apache.log4j.varia.NullAppender




@TestInstance(Lifecycle.PER_CLASS)
class VerificationSuiteTest {

    @Test
    fun `test multiple checks keyed`() {
        // Let's start by defining which checks we want to run
        // Define some checks we want to run for every row
        val rowLevelCheck = RowLevelCheck()
                // A check like this could have prevent a previous production issue with M2 data
                .isComplete("sessionId")
                .isContainedIn("categoryValue", listOf(ClickType.TypeA, ClickType.TypeB))
                .isInRange("nestedInfo.nestedDoubleValue", BigDecimal.valueOf(3.0), BigDecimal.valueOf(8.0))
                .matchesPattern("userId", Pattern.compile("user.*"))

        // Define some checks we want to run for windows of rows
        val windowCheck = AggregateCheck()
                .onWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .hasCompletenessBetween("nestedInfo.nestedIntValue", null, 0.6)
                .hasApproxUniquenessBetween("nestedInfo.nestedStringValue", null, 0.5)
                .hasApproxCountDistinctBetween("nestedInfo.nestedStringValue", null, 3)
                .hasApproxQuantileBetween("nestedInfo.nestedDoubleValue", 0.9, null, 10.0)

        // Define some checks that we want to run continuously that output the current status in
        // regular intervals. Here we trigger every 3 rows, but we can have, e.g.,
        // a ContinuousEventTimeTrigger that triggers every x seconds or minutes
        val continuousCheck = AggregateCheck()
                .onContinuousStreamWithTrigger(CountTrigger.of(3))
                .hasCompletenessBetween("sessionId", 0.9)
                .hasApproxUniquenessBetween("sessionId", 0.5)
                .hasApproxCountDistinctBetween("userId", null, 2)
                .hasApproxQuantileBetween("intValue", 0.5, 7.0)

        // Define some checks using an Anomaly Detection strategy. Here, we run the check on a completeness metric
        // that gets computed every 100ms. The anomaly detection check works based on the standard deviation of the
        // observed metrics.
        val anomalyCheck = AggregateAnomalyCheck()
            .onCompleteness("nestedInfo.nestedIntValue")
            .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
            .withStrategy(DetectionStrategy().onlineNormal(0.1, 1.0))
            .build()

        // Let's load the data and run the checks
        val (env, rawStream) = TestDataUtils.createEnvAndGetClickStream()
        val keyedRawStream = rawStream.keyBy { clickInfo -> clickInfo.userId }

        // add the row level checks, the window checks and the continuous checks
        val verificationResult = VerificationSuite()
                .onDataStream(keyedRawStream, env.config)
                .addRowLevelCheck(rowLevelCheck)
                .addAggregateCheck(windowCheck)
                .addAggregateCheck(continuousCheck)
                .addAnomalyCheck(anomalyCheck)
                .build()

        // Now we can use the verificationResult to get the output streams from our checks
        // Let's take a look at the RowLevelCheckResults
        val rowLevelResultStream = verificationResult.getResultsForCheck(rowLevelCheck)
        val rowRowResults = rowLevelResultStream!!.executeAndCollect().asSequence().toList()
        println("--- Row Level Check Results ---")
        println("Constraint: ${rowLevelCheck.constraints[0]}")
        rowRowResults
                .filter { checkResult -> !checkResult.constraintResults!![0].outcome!! }
                .take(5)
                .forEach { checkResult ->
                    val clickInfo = checkResult.checkedObject
                    val constraintResult = checkResult.constraintResults!![0]
                    println("Value: ${clickInfo?.sessionId}, Outcome: ${constraintResult.outcome}")
                }
        println("Constraint: ${rowLevelCheck.constraints[1]}")
        rowRowResults
                .filter { checkResult -> !checkResult.constraintResults!![1].outcome!! }
                .take(5)
                .forEach { checkResult ->
                    val clickInfo = checkResult.checkedObject
                    val constraintResult = checkResult.constraintResults!![1]
                    println("Value: ${clickInfo?.categoryValue}, Outcome: ${constraintResult.outcome}")
                }

        //  Let's take a look at the Window Check Results
        val windowCheckResultStream = verificationResult.getResultsForCheck(windowCheck)!!
        val firstWindowResults = windowCheckResultStream.executeAndCollect().asSequence().take(3)
        println("--- Window Check Results ---")
        println("Constraint: ${windowCheck.constraints[0]}")
        firstWindowResults.forEach { aggregateCheckResult ->
            println("Uniqueness: constraint is ${aggregateCheckResult.constraintResults!![1].outcome} for " +
                    "${aggregateCheckResult.constraintResults!![1].aggregate}")
        }

        // Let's take a look at the Continuous Check Results
        val continuousResultStream = verificationResult.getResultsForCheck(continuousCheck)!!
        val firstContinuousResults = continuousResultStream.executeAndCollect().asSequence().take(3)
        println("--- Continuous Check Results ---")
        println("Constraint: ${windowCheck.constraints[1]}")
        firstContinuousResults.forEach { aggregateCheckResult ->
            println("Distinct Items: constraint is ${aggregateCheckResult.constraintResults!![2].outcome} for " +
                    "${aggregateCheckResult.constraintResults!![2].aggregate}")
        }

        // Let's take a look at the Anomaly Check Results
        val anomalyResultStream = verificationResult.getResultsForCheck(anomalyCheck)!!
        val firstAnomalyResults = anomalyResultStream.executeAndCollect().asSequence().take(3)
        println("--- Anomaly Check Results ---")
        println("Strategy: ${(anomalyCheck as AggregateAnomalyDetector).strategy}")
        println("Metric: ${anomalyCheck.constraint}")
        firstAnomalyResults.forEach { aggregateCheckResult ->
            println("Completeness ${aggregateCheckResult.value!!} is an anomaly: ${aggregateCheckResult.isAnomaly!!}")
        }
    }
}
