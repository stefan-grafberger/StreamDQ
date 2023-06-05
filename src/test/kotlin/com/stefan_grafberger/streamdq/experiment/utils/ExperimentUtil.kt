package com.stefan_grafberger.streamdq.experiment.utils

import com.stefan_grafberger.streamdq.experiment.model.RedditPost
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

class ExperimentUtil {
    fun createStreamExecutionEnvironment(): StreamExecutionEnvironment {
        val conf = Configuration()
        conf.setInteger(RestOptions.PORT, 8081)
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.parallelism = 1
        return env
    }

    fun generateFileSourceFromPath(path: String): FileSource<RedditPost>? {
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

    inline fun <R> executeAndMeasureTimeMillis(block: () -> R): Pair<R, Long> {
        val start = System.currentTimeMillis()
        val result = block()
        return result to (System.currentTimeMillis() - start)
    }
}