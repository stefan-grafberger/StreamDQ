package com.stefan_grafberger.streamdq.experiment.model

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder

@JsonPropertyOrder(
        "id", "title", "score", "author", "authorFlairText", "removedBy", "totalAwardsReceived",
        "awarders", "createdUtc", "fullLink", "numComments", "over18")
data class RedditPost @JvmOverloads constructor(
        var id: String? = null,
        var title: String? = null,
        var score: String? = null,
        var author: String? = null,
        var authorFlairText: String? = null,
        var removedBy: String? = null,
        var totalAwardsReceived: String? = null,
        var awarders: String? = null,
        var createdUtc: String? = null,
        var fullLink: String? = null,
        var numComments: String? = null,
        var over18: String? = null)