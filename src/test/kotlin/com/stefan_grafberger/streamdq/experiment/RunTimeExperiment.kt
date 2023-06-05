package com.stefan_grafberger.streamdq.experiment

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.experiment.experimentlogger.ExperimentLogger
import com.stefan_grafberger.streamdq.experiment.model.RedditPost
import lombok.extern.slf4j.Slf4j
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.RestOptions
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.guava30.com.google.common.base.Function
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@Slf4j
class RunTimeExperiment {

    private val log = ExperimentLogger()

    @Test
    @Disabled
    fun testOnRedditDataSet() {
        //given
        val env = createStreamExecutionEnvironment()
        //setup deserialization configuration
        val source = generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/reddit_posts_12million.csv")
        //start generating stream and transformation
        val startTransformationTime = System.nanoTime()
        val redditPostStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Reddit Posts")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<RedditPost>()
                                .withTimestampAssigner { post, _ -> post.createdUtc!!.toLong() }
                )
        val detector = AggregateAnomalyCheck()
                .onCompleteness("removedBy")
                .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .withStrategy(DetectionStrategy().onlineNormal(0.0, 0.3))
                .build()
        //when
        val (actualAnomalies, detectDuration) = executeAndMeasureTimeMillis {
            detector.detectAnomalyStream(redditPostStream)
                    .filter { result -> result.isAnomaly!! }
        }
        //then
        val endToEndTransformationTime = System.nanoTime() - startTransformationTime
        log.info("End To End Transformation Time: " + TimeUnit.NANOSECONDS.toMillis(endToEndTransformationTime) + " ms")
        log.info("Anomaly Detection Transformation Time: $detectDuration ms")
        //sink
        actualAnomalies.print("anomaly stream output")
        val jobExecutionResult = env.execute()
        log.info("Net Fink Job Execution Run Time: ${jobExecutionResult.netRuntime} ms")
    }

    private fun createStreamExecutionEnvironment(): StreamExecutionEnvironment {
        val conf = Configuration()
        conf.setInteger(RestOptions.PORT, 8081)
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.parallelism = 1
        return env
    }

    private fun generateFileSourceFromPath(path: String): FileSource<RedditPost>? {
        val schemaGenerator = Function<CsvMapper, CsvSchema> { mapper ->
            mapper?.schemaFor(RedditPost::class.java)
                    ?.withQuoteChar('"')
                    ?.withColumnSeparator(',')
                    ?.withNullValue("")
                    ?.withSkipFirstDataRow(true)
        }
        val mapper = CsvMapper.builder()
                .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                .build()
        val csvFormat = CsvReaderFormat
                .forSchema(schemaGenerator.apply(mapper), TypeInformation.of(RedditPost::class.java))
        return FileSource
                .forRecordStreamFormat(csvFormat, Path(path))
                .build()
    }

    private inline fun <R> executeAndMeasureTimeMillis(block: () -> R): Pair<R, Long> {
        val start = System.currentTimeMillis()
        val result = block()
        return result to (System.currentTimeMillis() - start)
    }
}
