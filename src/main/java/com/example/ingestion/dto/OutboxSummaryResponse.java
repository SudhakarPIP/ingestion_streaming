package com.example.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OutboxSummaryResponse(
    @JsonProperty("tenantId")
    String tenantId,

    @JsonProperty("from")
    String from,

    @JsonProperty("to")
    String to,

    @JsonProperty("pending")
    Long pending,

    @JsonProperty("processing")
    Long processing,

    @JsonProperty("done")
    Long done,

    @JsonProperty("failed")
    Long failed,

    @JsonProperty("total")
    Long total
) {}
