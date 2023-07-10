# StreamDQ

## Description
StreamDQ is a library built on top of Apache Flink for defining "unit tests for data", which measure data quality in large data streams. 

## Example
```kotlin
import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck
import com.stefan_grafberger.streamdq.VerificationSuite

val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(LOCAL_PARALLELISM)
env.streamTimeCharacteristic = TimeCharacteristic.EventTime

val rawStream = ClickData.readData(env)
val keyedStream = rawStream.keyBy { data -> data.userId }

val verificationResult = VerificationSuite()
    .onDataStream(keyedStream, env.config)
    .addRowLevelCheck(RowLevelCheck()
        .isContainedIn("priority", listOf(Priority.HIGH, Priority.LOW))
        .isInRange("numViews", BigDecimal.valueOf(0), BigDecimal.valueOf(1_000_000)
        .matchesPattern("email", Pattern.compile(EMAIL_REGEX))))
    .addAggregateCheck(AggregateCheck()
        .onWindow(TumblingEventTimeWindows.of(Time.seconds(10)))
        .hasCompletenessBetween("productName", 0.8, 1.0)
        .hasApproxUniquenessBetween("id", 0.9, 1.0)
        .hasApproxQuantileBetween("numViews", 0.5, 0.0, 10.0))
    .addAggregateCheck(AggregateCheck()
        .onContinuousStreamWithTrigger(CountTrigger.of(100))
        .hasApproxCountDistinctBetween("productName", 5_000_000, 10_000_000))
    .addAnomalyCheck(AggregateAnomalyCheck()
        .onCompleteness("productId")
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(100)))
        .withStrategy(DetectionStrategy().onlineNormal(0.1, 1.0))
        .build())
    .build()                
```
