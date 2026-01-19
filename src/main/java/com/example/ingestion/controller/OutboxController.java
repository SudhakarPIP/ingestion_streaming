package com.example.ingestion.controller;

import com.example.ingestion.dto.BigQueryStatusResponse;
import com.example.ingestion.dto.OutboxSummaryResponse;
import com.example.ingestion.dto.RetryResponse;
import com.example.ingestion.service.BigQueryService;
import com.example.ingestion.service.OutboxService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/v1/outbox")
@Slf4j
public class OutboxController {

    private final OutboxService outboxService;
    private final BigQueryService bigQueryService;

    public OutboxController(OutboxService outboxService, BigQueryService bigQueryService) {
        this.outboxService = outboxService;
        this.bigQueryService = bigQueryService;
    }

    @GetMapping("/summary")
    public ResponseEntity<OutboxSummaryResponse> getSummary(
            @RequestParam(required = false, defaultValue = "") String tenantId,
            @RequestParam(required = false) 
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false) 
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        
        // Default values
        if (from == null) {
            from = LocalDateTime.now().minusDays(7);
        }
        if (to == null) {
            to = LocalDateTime.now();
        }

        log.info("Getting outbox summary: tenantId={}, from={}, to={}", tenantId, from, to);
        OutboxSummaryResponse summary = outboxService.getSummary(tenantId, from, to);
        return ResponseEntity.ok(summary);
    }

    @PostMapping("/retry")
    public ResponseEntity<RetryResponse> retryFailed(
            @RequestParam(required = false) String tenantId,
            @RequestParam(defaultValue = "100") int limit) {
        
        if (tenantId == null || tenantId.isEmpty()) {
            tenantId = "";
        }

        log.info("Retrying failed records: tenantId={}, limit={}", tenantId, limit);
        RetryResponse response = outboxService.retryFailed(tenantId, limit);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/bigquery/status")
    public ResponseEntity<BigQueryStatusResponse> getBigQueryStatus() {
        log.info("Getting BigQuery status");
        BigQueryStatusResponse status = bigQueryService.getStatus();
        return ResponseEntity.ok(status);
    }

    @GetMapping("/bigquery/verify/{tenantId}/{eventId}")
    public ResponseEntity<Boolean> verifyEventInBigQuery(
            @PathVariable String tenantId,
            @PathVariable String eventId) {
        log.info("Verifying event in BigQuery: tenantId={}, eventId={}", tenantId, eventId);
        boolean exists = bigQueryService.verifyEventExists(tenantId, eventId);
        return ResponseEntity.ok(exists);
    }

    @GetMapping("/bigquery/count")
    public ResponseEntity<Long> getBigQueryEventCount(
            @RequestParam(required = false, defaultValue = "") String tenantId) {
        log.info("Getting BigQuery event count: tenantId={}", tenantId);
        long count = bigQueryService.getEventCount(tenantId);
        return ResponseEntity.ok(count);
    }
}

