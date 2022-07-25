package com.stefan_grafberger.streamdq.benchmark

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WikiTraceBenchmark {

    private val resourceName: String = "/clickstream-zhwiki-2022-06.tsv"

    private fun getResourceFileName(): String {
        return javaClass.getResource(resourceName).path
    }

    @Test
    fun `test load WikiTrace`() {
        val env = StreamExecutionEnvironment.createLocalEnvironment(1)
        val inputStream: DataStream<WikiTrace> = env.addSource(WikiTraceSource(WikiTraceBenchmark().getResourceFileName()))
        val result = inputStream.executeAndCollect().iterator().asSequence().toList()
        println(result)
    }
}
