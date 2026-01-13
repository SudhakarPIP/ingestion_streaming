package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public record ErrorResponse(
    @JsonProperty("error")
    String error,

    @JsonProperty("message")
    String message,

    @JsonProperty("timestamp")
    LocalDateTime timestamp,

    @JsonProperty("path")
    String path,

    @JsonProperty("correlationId")
    String correlationId
) {}
