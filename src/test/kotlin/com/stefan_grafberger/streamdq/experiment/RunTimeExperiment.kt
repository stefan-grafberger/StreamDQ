package com.stefan_grafberger.streamdq.experiment

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import com.stefan_grafberger.streamdq.checks.aggregate.ApproxUniquenessConstraint
import com.stefan_grafberger.streamdq.experiment.experimentlogger.ExperimentLogger
import com.stefan_grafberger.streamdq.experiment.model.RedditPost
import com.stefan_grafberger.streamdq.experiment.utils.ExperimentUtil
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class RunTimeExperiment {

    private var log = ExperimentLogger()
    private var util = ExperimentUtil()

    @Test
    @Disabled
    fun testAnomalyDetectionRunTimeOnRedditDataSetEndToEnd() {
        //given
        val env = util.createStreamExecutionEnvironment()
        val detector = AggregateAnomalyCheck()
                .onApproxUniqueness("score")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
                .withStrategy(DetectionStrategy().onlineNormal(0.0, 0.3))
                .build()
        //setup deserialization
        val source = util.generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/100M_reddit_posts.csv")
        val startTransformationTime = System.nanoTime()
        val redditPostStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Reddit Posts")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<RedditPost>()
                                .withTimestampAssigner { post, _ -> post.createdUtc!!.toLong() }
                )
        //when
        val (actualAnomalies, detectDuration) = util.executeAndMeasureTimeMillis {
            detector.detectAnomalyStream(redditPostStream)
        }
        //then
        val endToEndTransformationTime = System.nanoTime() - startTransformationTime
        //sink
        actualAnomalies.print("AnomalyCheckResult stream output")
        val jobExecutionResult = env.execute()
        log.info("End To End Transformation Time: " + TimeUnit.NANOSECONDS.toMillis(endToEndTransformationTime) + " ms")
        log.info("End to End Anomaly Detection Transformation Time(Latency): $detectDuration ms")
        log.info("Net Fink Job Execution Run Time: ${jobExecutionResult.netRuntime} ms")
    }

    @Test
    @Disabled
    fun testRunTimeOnRedditDataSetWithOnlyAggregation() {
        //given
        val env = util.createStreamExecutionEnvironment()
        //setup deserialization
        val source = util.generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/100M_reddit_posts.csv")
        val startTransformationTime = System.nanoTime()
        val redditPostStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Reddit Posts")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<RedditPost>()
                                .withTimestampAssigner { post, _ -> post.createdUtc!!.toLong() }
                )
        //when
        val (actualAnomalies, detectDuration) = util.executeAndMeasureTimeMillis {
            redditPostStream
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
                    .aggregate(ApproxUniquenessConstraint("score").getAggregateFunction(TypeInformation.of(RedditPost::class.java), env.config))
                    .returns(AggregateConstraintResult::class.java)
        }
        //then
        val endToEndTransformationTime = System.nanoTime() - startTransformationTime
        //sink
        actualAnomalies.print("AggregateConstraintResult stream output")
        val jobExecutionResult = env.execute()
        log.info("End To End Transformation Time: " + TimeUnit.NANOSECONDS.toMillis(endToEndTransformationTime) + " ms")
        log.info("Aggregation Constraint Function Transformation Time[Latency]: $detectDuration ms")
        log.info("Net Fink Job Execution Run Time: ${jobExecutionResult.netRuntime} ms")
    }

    @Test
    @Disabled
    fun testRunTimeOnRedditDataSetWithOnlyStreamLoading() {
        //given
        val env = util.createStreamExecutionEnvironment()
        //setup deserialization
        val source = util.generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/20M_reddit_posts.csv")
        val startTransformationTime = System.nanoTime()
        //when
        val redditPostStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Reddit Posts")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<RedditPost>()
                                .withTimestampAssigner { post, _ -> post.createdUtc!!.toLong() }
                )
        //then
        val endToEndTransformationTime = System.nanoTime() - startTransformationTime
        //sink
        redditPostStream.print("RedditPostStream output")
        val jobExecutionResult = env.execute()
        log.info("End To End Transformation Time: " + TimeUnit.NANOSECONDS.toMillis(endToEndTransformationTime) + " ms")
        log.info("Net Fink Job Execution Run Time: ${jobExecutionResult.netRuntime} ms")
    }
}
