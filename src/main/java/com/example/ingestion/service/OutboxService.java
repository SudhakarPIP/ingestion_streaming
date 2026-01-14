package com.example.ingestion.service;

import com.example.ingestion.dto.OutboxSummaryResponse;
import com.example.ingestion.dto.RetryResponse;
import com.example.ingestion.entity.EventInbox;
import com.example.ingestion.entity.EventOutbox;
import com.example.ingestion.entity.EventOutbox.OutboxStatus;
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
        
        outboxValidator.validateTenantId(tenantId);
        
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
                    tenantId,
                    from.toString(),
                    to.toString(),
                    pending,
                    processing,
                    done,
                    failed,
                    total
            );
        } catch (DataAccessException e) {
            log.error("Database error retrieving outbox summary: tenantId={}", tenantId, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
    }

    @Transactional
    public RetryResponse retryFailed(String tenantId, int limit) {
        outboxValidator.validateRetryLimit(limit);
        outboxValidator.validateTenantId(tenantId);
        
        try {
            Pageable pageable = PageRequest.of(0, limit);
            List<EventOutbox> failedRecords = eventOutboxRepository.findFailedByTenantIdForRetry(
                tenantId, OutboxStatus.FAILED, pageable);

            int retriedCount = 0;
            for (EventOutbox outbox : failedRecords) {
                outbox.setStatus(OutboxStatus.PENDING);
                outbox.setLastError(null);
                outbox.setNextRetryAt(LocalDateTime.now());
                eventOutboxRepository.save(outbox);
                retriedCount++;
            }

            log.info("Retried {} failed records for tenant: {}", retriedCount, tenantId);

            return new RetryResponse(
                    tenantId,
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
        int claimed = eventOutboxRepository.claimPendingRows(instanceId, lockTtlSeconds, batchSize);
        if (claimed > 0) {
            outboxClaimedTotal.increment(claimed);
        }
        return claimed;
    }

    @Transactional
    public List<EventOutbox> getClaimedRows(String instanceId, int lockTtlSeconds) {
        List<Object[]> rawRows = eventOutboxRepository.findClaimedRows(instanceId, lockTtlSeconds);
        return rawRows.stream()
                .map(row -> {
                    Long id = ((Number) row[0]).longValue();
                    return eventOutboxRepository.findById(id).orElse(null);
                })
                .filter(outbox -> outbox != null)
                .toList();
    }

    @Transactional
    public void processOutboxRecord(EventOutbox outbox, BigQueryService bigQueryService, int maxRetryCount) {
        // Set MDC for logging
        MDC.put("tenantId", outbox.getTenantId());
        MDC.put("eventId", outbox.getEventId());
        MDC.put("outboxId", String.valueOf(outbox.getId()));
        
        try {
            // Get associated event
            EventInbox eventInbox = eventInboxRepository.findByTenantIdAndEventId(
                outbox.getTenantId(), outbox.getEventId())
                .orElseThrow(() -> new RuntimeException("Event not found: " + outbox.getEventId()));

            // Stream insert to BigQuery
            bigQueryService.streamInsert(eventInbox);

            // Update status to DONE
            outbox.setStatus(OutboxStatus.DONE);
            outbox.setLastError(null);
            outbox.setLockedBy(null);
            outbox.setLockedAt(null);
            eventOutboxRepository.save(outbox);

            outboxDoneTotal.increment();
            log.info("Successfully processed outbox record: eventId={}, outboxId={}", 
                outbox.getEventId(), outbox.getId());
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
            
            eventOutboxRepository.save(outbox);
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
