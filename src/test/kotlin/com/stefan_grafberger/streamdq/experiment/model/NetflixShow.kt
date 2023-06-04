package com.stefan_grafberger.streamdq.experiment.model

import lombok.Data
import lombok.NoArgsConstructor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder

@Data
@NoArgsConstructor
@JsonPropertyOrder(
        "showId", "type", "title", "director", "cast", "country",
        "dateAdded", "releaseYear", "rating", "duration", "listedIn", "description")
data class NetflixShow(val showId: String? = "",
                       val type: String? = "",
                       val title: String? = "",
                       val director: String? = "",
                       val cast: String? = "",
                       val country: String? = "",
                       val dateAdded: String? = "",
                       val releaseYear: String? = "",
                       val rating: String? = "",
                       val duration: String? = "",
                       val listedIn: String? = "",
                       val description: String? = "")
