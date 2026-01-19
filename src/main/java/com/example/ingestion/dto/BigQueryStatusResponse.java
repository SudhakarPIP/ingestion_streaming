package com.example.ingestion.dto;

import java.time.LocalDateTime;

public record BigQueryStatusResponse(
    boolean available,
    boolean skipIfUnavailable,
    boolean bigQueryUnavailable,
    String projectId,
    String dataset,
    String table,
    String message
) {}

