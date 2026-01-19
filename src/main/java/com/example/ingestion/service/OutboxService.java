package com.example.ingestion.service;

import com.example.ingestion.dto.OutboxSummaryResponse;
import com.example.ingestion.dto.RetryResponse;
import com.example.ingestion.entity.EventInbox;
import com.example.ingestion.entity.EventOutbox;
import com.example.ingestion.entity.EventOutbox.OutboxStatus;
import com.example.ingestion.exception.EventNotFoundException;
import com.example.ingestion.repository.EventInboxRepository;
import com.example.ingestion.repository.EventOutboxRepository;
import com.example.ingestion.util.Constants;
import com.example.ingestion.util.RetryBackoffCalculator;
import com.example.ingestion.validator.OutboxValidator;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
public class OutboxService {

    private final EventOutboxRepository eventOutboxRepository;
    private final EventInboxRepository eventInboxRepository;
    private final Counter outboxClaimedTotal;
    private final Counter outboxDoneTotal;
    private final Counter outboxFailedTotal;
    private final OutboxValidator outboxValidator;

    public OutboxService(EventOutboxRepository eventOutboxRepository,
                        EventInboxRepository eventInboxRepository,
                        Counter outboxClaimedTotal,
                        Counter outboxDoneTotal,
                        Counter outboxFailedTotal,
                        OutboxValidator outboxValidator) {
        this.eventOutboxRepository = eventOutboxRepository;
        this.eventInboxRepository = eventInboxRepository;
        this.outboxClaimedTotal = outboxClaimedTotal;
        this.outboxDoneTotal = outboxDoneTotal;
        this.outboxFailedTotal = outboxFailedTotal;
        this.outboxValidator = outboxValidator;
    }

    public OutboxSummaryResponse getSummary(String tenantId, LocalDateTime from, LocalDateTime to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("From and to dates cannot be null");
        }
        
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("From date cannot be after to date");
        }
        
        // Validate tenantId only if provided (empty string means all tenants)
        if (tenantId != null && !tenantId.isEmpty()) {
            outboxValidator.validateTenantId(tenantId);
        }
        
        try {
            Long pending = eventOutboxRepository.countByTenantIdAndCreatedAtBetweenAndStatus(
                tenantId, from, to, OutboxStatus.PENDING);
            Long processing = eventOutboxRepository.countByTenantIdAndCreatedAtBetweenAndStatus(
                tenantId, from, to, OutboxStatus.PROCESSING);
            Long done = eventOutboxRepository.countByTenantIdAndCreatedAtBetweenAndStatus(
                tenantId, from, to, OutboxStatus.DONE);
            Long failed = eventOutboxRepository.countByTenantIdAndCreatedAtBetweenAndStatus(
                tenantId, from, to, OutboxStatus.FAILED);

            Long total = pending + processing + done + failed;

            return new OutboxSummaryResponse(
                    tenantId != null ? tenantId : "",
                    from.toString(),
                    to.toString(),
                    pending,
                    processing,
                    done,
                    failed,
                    total
            );
        } catch (DataAccessException e) {
            log.error("Database error retrieving outbox summary: tenantId={}, from={}, to={}", tenantId, from, to, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
    }

    @Transactional
    public RetryResponse retryFailed(String tenantId, int limit) {
        outboxValidator.validateRetryLimit(limit);
        
        // Validate tenantId only if provided (empty string means all tenants)
        if (tenantId != null && !tenantId.isEmpty()) {
            outboxValidator.validateTenantId(tenantId);
        }
        
        try {
            Pageable pageable = PageRequest.of(0, limit);
            List<EventOutbox> failedRecords = eventOutboxRepository.findFailedByTenantIdForRetry(
                tenantId, OutboxStatus.FAILED, pageable);

            if (failedRecords.isEmpty()) {
                log.info("No failed records found to retry for tenant: {}", tenantId != null ? tenantId : "all");
                return new RetryResponse(
                        tenantId != null ? tenantId : "",
                        0,
                        "No failed records found to retry"
                );
            }

            int retriedCount = 0;
            for (EventOutbox outbox : failedRecords) {
                if (outbox == null) {
                    continue;
                }
                outbox.setStatus(OutboxStatus.PENDING);
                outbox.setLastError(null);
                outbox.setRetryCount(0);
                outbox.setNextRetryAt(LocalDateTime.now());
                eventOutboxRepository.save(outbox);
                retriedCount++;
            }

            log.info("Retried {} failed records for tenant: {}", retriedCount, tenantId != null ? tenantId : "all");

            return new RetryResponse(
                    tenantId != null ? tenantId : "",
                    retriedCount,
                    "Retried " + retriedCount + " failed records"
            );
        } catch (DataAccessException e) {
            log.error("Database error retrying failed records: tenantId={}, limit={}", tenantId, limit, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
    }

    @Transactional
    public int claimPendingRows(String instanceId, int lockTtlSeconds, int batchSize) {
        if (instanceId == null || instanceId.trim().isEmpty()) {
            throw new IllegalArgumentException("Instance ID cannot be null or empty");
        }
        if (lockTtlSeconds <= 0) {
            throw new IllegalArgumentException("Lock TTL must be greater than 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than 0");
        }
        
        try {
            int claimed = eventOutboxRepository.claimPendingRows(instanceId, lockTtlSeconds, batchSize);
            if (claimed > 0) {
                outboxClaimedTotal.increment(claimed);
            }
            return claimed;
        } catch (DataAccessException e) {
            log.error("Database error claiming pending rows: instanceId={}, lockTtlSeconds={}, batchSize={}", 
                instanceId, lockTtlSeconds, batchSize, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
    }

    @Transactional
    public List<EventOutbox> getClaimedRows(String instanceId, int lockTtlSeconds) {
        if (instanceId == null || instanceId.trim().isEmpty()) {
            throw new IllegalArgumentException("Instance ID cannot be null or empty");
        }
        if (lockTtlSeconds <= 0) {
            throw new IllegalArgumentException("Lock TTL must be greater than 0");
        }
        
        try {
            List<Object[]> rawRows = eventOutboxRepository.findClaimedRows(instanceId, lockTtlSeconds);
            if (rawRows == null || rawRows.isEmpty()) {
                return List.of();
            }
            
            return rawRows.stream()
                    .filter(row -> row != null && row.length > 0 && row[0] != null)
                    .map(row -> {
                        try {
                            Long id = ((Number) row[0]).longValue();
                            return eventOutboxRepository.findById(id).orElse(null);
                        } catch (Exception e) {
                            log.warn("Error converting row to EventOutbox: {}", e.getMessage());
                            return null;
                        }
                    })
                    .filter(outbox -> outbox != null)
                    .toList();
        } catch (DataAccessException e) {
            log.error("Database error retrieving claimed rows: instanceId={}, lockTtlSeconds={}", instanceId, lockTtlSeconds, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
    }

    @Transactional
    public void processOutboxRecord(EventOutbox outbox, BigQueryService bigQueryService, int maxRetryCount) {
        if (outbox == null) {
            log.error("Cannot process null outbox record");
            return;
        }
        if (bigQueryService == null) {
            log.error("BigQueryService cannot be null");
            throw new IllegalArgumentException("BigQueryService cannot be null");
        }
        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("Max retry count cannot be negative");
        }
        
        // Set MDC for logging
        MDC.put("tenantId", outbox.getTenantId());
        MDC.put("eventId", outbox.getEventId());
        MDC.put("outboxId", String.valueOf(outbox.getId()));
        
        try {
            // Validate outbox state
            if (outbox.getTenantId() == null || outbox.getTenantId().trim().isEmpty()) {
                throw new IllegalArgumentException("Outbox record has invalid tenantId");
            }
            if (outbox.getEventId() == null || outbox.getEventId().trim().isEmpty()) {
                throw new IllegalArgumentException("Outbox record has invalid eventId");
            }
            
            // Get associated event
            EventInbox eventInbox = eventInboxRepository.findByTenantIdAndEventId(
                outbox.getTenantId(), outbox.getEventId())
                .orElseThrow(() -> new EventNotFoundException(outbox.getTenantId(), outbox.getEventId()));

            // Insert to BigQuery
            bigQueryService.streamInsert(eventInbox);

            // Update status to DONE
            outbox.setStatus(OutboxStatus.DONE);
            outbox.setLastError(null);
            outbox.setLockedBy(null);
            outbox.setLockedAt(null);
            try {
                eventOutboxRepository.save(outbox);
            } catch (DataAccessException e) {
                log.error("Database error saving outbox record after successful processing: eventId={}, outboxId={}", 
                    outbox.getEventId(), outbox.getId(), e);
                throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
            }

            outboxDoneTotal.increment();
            log.info("Successfully processed outbox record: eventId={}, outboxId={}", 
                outbox.getEventId(), outbox.getId());
        } catch (EventNotFoundException e) {
            log.error("Event not found for outbox record: eventId={}, outboxId={}", 
                outbox.getEventId(), outbox.getId(), e);
            outbox.setStatus(OutboxStatus.FAILED);
            outbox.setLastError(truncateError("Event not found: " + e.getMessage()));
            outbox.setLockedBy(null);
            outbox.setLockedAt(null);
            outboxFailedTotal.increment();
            try {
                eventOutboxRepository.save(outbox);
            } catch (DataAccessException dbEx) {
                log.error("Database error saving outbox record after error: eventId={}, outboxId={}", 
                    outbox.getEventId(), outbox.getId(), dbEx);
            }
        } catch (Exception e) {
            log.error("Error processing outbox record: eventId={}, outboxId={}, error={}", 
                outbox.getEventId(), outbox.getId(), e.getMessage(), e);
            
            int newRetryCount = outbox.getRetryCount() + 1;
            
            if (newRetryCount >= maxRetryCount) {
                // Permanent failure - mark as FAILED with no retry
                outbox.setStatus(OutboxStatus.FAILED);
                outbox.setLastError(truncateError(e.getMessage()));
                outbox.setLockedBy(null);
                outbox.setLockedAt(null);
                outboxFailedTotal.increment();
                log.warn("Outbox record exceeded max retry count: eventId={}, outboxId={}, retryCount={}", 
                    outbox.getEventId(), outbox.getId(), newRetryCount);
            } else {
                // Transient failure - schedule retry with exponential backoff
                outbox.setStatus(OutboxStatus.FAILED);
                outbox.setLastError(truncateError(e.getMessage()));
                outbox.setRetryCount(newRetryCount);
                outbox.setNextRetryAt(RetryBackoffCalculator.calculateNextRetryAt(outbox.getRetryCount()));
                outbox.setLockedBy(null);
                outbox.setLockedAt(null);
                outboxFailedTotal.increment();
                log.info("Scheduled retry for outbox record: eventId={}, outboxId={}, retryCount={}, nextRetryAt={}", 
                    outbox.getEventId(), outbox.getId(), outbox.getRetryCount(), outbox.getNextRetryAt());
            }
            
            try {
                eventOutboxRepository.save(outbox);
            } catch (DataAccessException dbEx) {
                log.error("Database error saving outbox record after failure: eventId={}, outboxId={}", 
                    outbox.getEventId(), outbox.getId(), dbEx);
                throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, dbEx);
            }
        } finally {
            MDC.remove("tenantId");
            MDC.remove("eventId");
            MDC.remove("outboxId");
        }
    }

    private String truncateError(String error) {
        if (error == null) {
            return null;
        }
        if (error.length() > 2048) {
            return error.substring(0, 2045) + "...";
        }
        return error;
    }
}
