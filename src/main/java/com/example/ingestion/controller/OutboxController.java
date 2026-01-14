package com.example.ingestion.controller;

import com.example.ingestion.dto.OutboxSummaryResponse;
import com.example.ingestion.dto.RetryResponse;
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

    public OutboxController(OutboxService outboxService) {
        this.outboxService = outboxService;
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
}

