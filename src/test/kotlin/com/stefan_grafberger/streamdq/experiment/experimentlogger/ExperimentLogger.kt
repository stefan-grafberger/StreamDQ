package com.stefan_grafberger.streamdq.experiment.experimentlogger

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ExperimentLogger {

    private val logger: Logger = LoggerFactory.getLogger(ExperimentLogger::class.java)

    fun debug(logging: String) {
        logger.debug("\u001B[34m$logging\u001B[0m")
    }

    fun info(logging: String) {
        logger.info("\u001B[32m$logging\u001B[0m")
    }

    fun error(logging: String) {
        logger.error("\u001B[31m$logging\u001B[0m")
    }
}