package com.stefan_grafberger.streamdq.experiment.model

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder

@JsonPropertyOrder(
        "marketplace", "customerId", "reviewId", "productId", "productParent",
        "productTitle", "productCategory", "starRating", "helpfulVotes", "totalVotes",
        "vine", "verifiedPurchase", "reviewHeadline", "reviewBody", "reviewDate")
data class AmazonReview @JvmOverloads constructor(
        var marketplace: String? = null,
        var customerId: String? = null,
        var reviewId: String? = null,
        var productId: String? = null,
        var productParent: String? = null,
        var productTitle: String? = null,
        var productCategory: String? = null,
        var starRating: String? = null,
        var helpfulVotes: String? = null,
        var totalVotes: String? = null,
        var vine: String? = null,
        var verifiedPurchase: String? = null,
        var reviewHeadline: String? = null,
        var reviewBody: String? = null,
        var reviewDate: String? = null,
)