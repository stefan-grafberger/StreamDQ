package com.stefan_grafberger.streamdq.experiment

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.experiment.model.RedditPost
import com.stefan_grafberger.streamdq.experiment.watermarkgenerator.TimeLagWatermarkGenerator
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

class RunTimeExperiment {

    @Test
    @Disabled
    fun testTransferringNetflixCsvToStream() {
        val conf = Configuration()
        conf.setInteger(RestOptions.PORT, 8081)
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.parallelism = 1
        val source = generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/netflix_movies_tvs.csv")
        val csvInputStream = env.fromSource(source, WatermarkStrategy.forGenerator { TimeLagWatermarkGenerator() }, "Netflix Show")
        csvInputStream.print("csvInputStream")
        env.execute("flink-csv-reader")
    }

    @Test
    @Disabled
    fun testOnRedditDataSet() {
        //prepare env
        val conf = Configuration()
        conf.setInteger(RestOptions.PORT, 8081)
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.parallelism = 1
        //setup deserialization configuration
        val source = generateFileSourceFromPath("src/test/kotlin/com/stefan_grafberger/streamdq/experiment/dataset/reddit_posts_12million.csv")
        val start = System.nanoTime()
        //generate stream
        val redditPostStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Reddit Posts")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps<RedditPost>()
                                .withTimestampAssigner { post, _ -> post.createdUtc!!.toLong() }
                )
        //given
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
        val endToEndTime = System.nanoTime() - start
        //then
        println("End To End Time: " + TimeUnit.NANOSECONDS.toMillis(endToEndTime) + " ms")
        println("Anomaly Detection Time: $detectDuration ms")
        val (response, executionTime) = executeAndMeasureTimeMillis {
            actualAnomalies.print("anomaly stream output")
            env.execute()
        }
        println("Net Fink Job Run Time: ${response.netRuntime} ms")
        println("Whole Job Execution Time: $executionTime ms")
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
