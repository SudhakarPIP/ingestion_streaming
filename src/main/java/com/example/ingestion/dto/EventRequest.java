package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDateTime;
import java.util.Map;

public record EventRequest(
    @NotBlank(message = "tenantId is required")
    @JsonProperty("tenantId")
    String tenantId,

    @NotBlank(message = "eventId is required")
    @JsonProperty("eventId")
    String eventId,

    @NotBlank(message = "eventType is required")
    @JsonProperty("eventType")
    String eventType,

    @NotNull(message = "eventTs is required")
    @JsonProperty("eventTs")
    LocalDateTime eventTs,

    @NotNull(message = "payload is required")
    @JsonProperty("payload")
    Map<String, Object> payload,

    @JsonProperty("source")
    String source
) {}
