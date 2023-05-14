package com.stefan_grafberger.streamdq.data

import com.stefan_grafberger.streamdq.TestUtils
import com.stefan_grafberger.streamdq.checks.AggregateConstraintResult
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import kotlin.random.Random
import kotlin.random.asJavaRandom

enum class ClickType { TypeA, TypeB, TypeC }

data class NestedClickInfo @JvmOverloads constructor(
        var nestedIntValue: Int? = 0,
        var nestedDoubleValue: Double = 0.0,
        var nestedStringValue: String = "xy",
        var nestedListValue: List<String> = ArrayList()
)

data class ClickInfo @JvmOverloads constructor(
        var userId: String = "",
        var sessionId: String? = "",
        var timestamp: Long = 0,
        var intValue: Int = 0,
        var categoryValue: ClickType = ClickType.TypeA,
        var doubleValue: Double = 0.0,
        var nestedInfo: NestedClickInfo = NestedClickInfo()
)

object TestDataUtils {
    fun createEnvAndGetClickStream(): Pair<StreamExecutionEnvironment, SingleOutputStreamOperator<ClickInfo>> {
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val clickStream = environment.fromElements(
                // First 100 ms window
                ClickInfo(
                        "userA", "session-0", 10000, 2, ClickType.TypeA, 10.0,
                        NestedClickInfo(20, 5.5, "a", listOf())
                ),
                ClickInfo(
                        "UserB", "session-2", 10005, 5, ClickType.TypeB, 0.0,
                        NestedClickInfo(23, 2.8, "a", listOf("c"))
                ),
                ClickInfo(
                        "UserB", "session-2", 10020, 10, ClickType.TypeA, 5.24,
                        NestedClickInfo(null, 6.1, "a", listOf("a", "b", "c"))
                ),
                ClickInfo(
                        "userA", "session-0", 10091, 20, ClickType.TypeC, 10.2,
                        NestedClickInfo(23, 7.2, "a", listOf("a"))
                ),
                ClickInfo(
                        "userA", null, 10095, 21, ClickType.TypeA, 8.7,
                        NestedClickInfo(11, 8.6, "b", listOf())
                ),
                // Second 100 ms window
                ClickInfo(
                        "userC", "session-3", 10125, 9, ClickType.TypeB, 1.24,
                        NestedClickInfo(null, 9.6, "a", listOf("c"))
                ),
                ClickInfo(
                        "UserB", "session-3", 10130, 8, ClickType.TypeA, 4.8,
                        NestedClickInfo(null, 13.9, "c", listOf("a", "b", "c"))
                ),
                ClickInfo(
                        "UserB", "session-2", 10155, 7, ClickType.TypeC, 9.1,
                        NestedClickInfo(92, 15.1, "b", listOf("c"))
                ),
                ClickInfo(
                        "userA", "session-4", 10158, 3, ClickType.TypeA, 2.45,
                        NestedClickInfo(null, 2.3, "d", listOf("a", "b", "c"))
                ),
                // Third 100 ms window
                ClickInfo(
                        "UserB", "session-2", 10590, 1, ClickType.TypeA, 3.01,
                        NestedClickInfo(null, 4.2, "d", listOf("a", "b", "c"))
                ),
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps<ClickInfo>()
                        .withTimestampAssigner { clickInfo, _ -> clickInfo.timestamp }
        )
        return Pair(environment, clickStream)
    }

    fun createEnvAndGetAggregateResult(): Pair<StreamExecutionEnvironment, SingleOutputStreamOperator<AggregateConstraintResult>> {
        val environment = StreamExecutionEnvironment.createLocalEnvironment(TestUtils.LOCAL_PARALLELISM)
        val randomNum = Random(1)
        val dataSeriesList = MutableList(50) { _ -> randomNum.asJavaRandom().nextGaussian() }

        for (i in 20..30) {
            dataSeriesList[i] = dataSeriesList[i] + i + (i % 2 * -2 * i)
        }

        val aggregateConstraintResultList = MutableList(50) { index -> AggregateConstraintResult(true, dataSeriesList[index], "test") }
        val aggregateResultStream = environment.fromCollection(aggregateConstraintResultList)

        return Pair(environment, aggregateResultStream)
    }
}
