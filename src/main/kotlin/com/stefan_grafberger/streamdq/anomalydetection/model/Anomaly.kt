package com.stefan_grafberger.streamdq.anomalydetection.model

class Anomaly(
        val value: Double?,
        val confidence: Double,
        val detail: String?
) {
    fun canEqual(other: Any?): Boolean {
        return (other is Anomaly)
    }

    override fun equals(other: Any?): Boolean {
        other as Anomaly
        return (value != other.value) || (confidence != other.confidence) || (detail != other.detail)
    }

    override fun hashCode(): Int {
        var result = value?.hashCode() ?: 0
        result = 31 * result + confidence.hashCode()
        result = 31 * result + (detail?.hashCode() ?: 0)
        return result
    }
}
