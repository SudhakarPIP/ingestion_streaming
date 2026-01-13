package com.example.ingestion.dto;

import com.example.ingestion.entity.EventOutbox.OutboxStatus;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Map;

public record EventResponse(
    @JsonProperty("tenantId")
    String tenantId,

    @JsonProperty("eventId")
    String eventId,

    @JsonProperty("eventType")
    String eventType,

    @JsonProperty("eventTs")
    LocalDateTime eventTs,

    @JsonProperty("payload")
    Map<String, Object> payload,

    @JsonProperty("source")
    String source,

    @JsonProperty("status")
    OutboxStatus status,

    @JsonProperty("createdAt")
    LocalDateTime createdAt
) {}
