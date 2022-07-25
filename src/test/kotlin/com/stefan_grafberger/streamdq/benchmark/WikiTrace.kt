package com.stefan_grafberger.streamdq.benchmark

import de.bytefish.jtinycsvparser.CsvParser
import de.bytefish.jtinycsvparser.CsvParserOptions
import de.bytefish.jtinycsvparser.mapping.CsvMapping
import de.bytefish.jtinycsvparser.tokenizer.StringSplitTokenizer
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.lang.Exception
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.nio.file.Path

data class WikiTrace @JvmOverloads constructor(
    var counter: Int? = null,
    var timestamp: String? = null,
    var url: String? = null,
    var dbUpdate: String? = null
)

class WikiTraceMapper(creator: () -> WikiTrace) : CsvMapping<WikiTrace>(creator) {
    init {
        mapProperty(0, Int::class.javaObjectType) { wikiTrace: WikiTrace?, intValue: Int -> wikiTrace?.counter = intValue }
        mapProperty(1, String::class.javaObjectType) { wikiTrace: WikiTrace?, stringValue: String -> wikiTrace?.timestamp = stringValue }
        mapProperty(2, String::class.javaObjectType) { wikiTrace: WikiTrace?, stringValue: String -> wikiTrace?.url = stringValue }
        mapProperty(3, String::class.javaObjectType) { wikiTrace: WikiTrace?, stringValue: String -> wikiTrace?.dbUpdate = stringValue }
    }
}

class WikiTraceSource(private var sourceFilePath: String) : SourceFunction<WikiTrace> {
    @Volatile
    private var isRunning = true
    private var counter = 0

    @Throws(Exception::class)
    override fun run(sourceContext: SourceContext<WikiTrace?>) {
        val csvSourcePath: Path = FileSystems.getDefault().getPath(sourceFilePath)
        createWikiTraceParser()
            .readFromFile(csvSourcePath, StandardCharsets.UTF_8) // Really old WikiTrace files may use ISO_8859_1
            .filter { x -> x.isValid }
            .map { x -> x.result }
            .use { stream ->
                val iterator: Iterator<WikiTrace?> = stream.iterator()
                while (isRunning && iterator.hasNext()) {
                    sourceContext.collect(iterator.next())
                    counter++
                }
            }
    }

    override fun cancel() {
        isRunning = false
    }

    companion object {
        fun createWikiTraceParser(): CsvParser<WikiTrace?> {
            return CsvParser(
                CsvParserOptions(
                    false,
                    StringSplitTokenizer("\\t", false),
                ),
                WikiTraceMapper { WikiTrace() }
            )
        }
    }
}
