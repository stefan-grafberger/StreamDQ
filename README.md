StreamDQ
================================

[![StreamDQ](https://img.shields.io/badge/🔎-StreamDQ-green)](https://github.com/stefan-grafberger/StreamDQ)
[![GitHub license](https://img.shields.io/badge/License-Apache%202.0-yellowgreen.svg)](https://github.com/stefan-grafberger/StreamDQ/blob/master/LICENSE)
[![Build Status](https://github.com/stefan-grafberger/mlinspect/actions/workflows/build.yml/badge.svg)](https://github.com/stefan-grafberger/StreamDQ/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/stefan-grafberger/StreamDQ/branch/main/graph/badge.svg?token=KTMNPBV1ZZ)](https://codecov.io/gh/stefan-grafberger/StreamDQ)

StreamDQ is a library built on top of Apache Flink for defining "unit tests for data", which measure data quality in large data streams. 

## Run StreamDQ locally

Prerequisite: Java 11, Maven

1. Clone this repository
2. Switch to its directory

   `cd StreamDQ` <br>
3. Install and run the tests

   `mvn install` <br>

## How to use StreamDQ
```kotlin 
import com.stefan_grafberger.streamdq.VerificationSuite
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck

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
    .build()                
```


## License
This library is licensed under the Apache 2.0 License.
