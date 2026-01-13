package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RetryResponse(
    @JsonProperty("tenantId")
    String tenantId,

    @JsonProperty("retriedCount")
    Integer retriedCount,

    @JsonProperty("message")
    String message
) {}
