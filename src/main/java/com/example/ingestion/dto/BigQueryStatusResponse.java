package com.example.ingestion.dto;

public record BigQueryStatusResponse(
    boolean available,
    boolean skipIfUnavailable,
    boolean bigQueryUnavailable,
    String projectId,
    String dataset,
    String table,
    String message
) {}

