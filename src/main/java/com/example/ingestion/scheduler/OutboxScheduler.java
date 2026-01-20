package com.example.ingestion.scheduler;

import com.example.ingestion.entity.EventOutbox;
import com.example.ingestion.service.BigQueryService;
import com.example.ingestion.service.OutboxService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class OutboxScheduler {

    private final OutboxService outboxService;
    private final BigQueryService bigQueryService;

    @Value("${worker.batch-size:200}")
    private int batchSize;

    @Value("${worker.lock-ttl-seconds:300}")
    private int lockTtlSeconds;

    @Value("${worker.instance-id:default-instance}")
    private String instanceId;

    @Value("${worker.max-retry-count:5}")
    private int maxRetryCount;

    public OutboxScheduler(OutboxService outboxService, BigQueryService bigQueryService) {
        this.outboxService = outboxService;
        this.bigQueryService = bigQueryService;
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox.fixed-delay:5000}", 
               initialDelayString = "${scheduler.outbox.initial-delay:10000}")
    public void processOutboxRecords() {
        try {
            if (instanceId == null || instanceId.trim().isEmpty()) {
                log.error("Invalid instance ID configuration: {}", instanceId);
                return;
            }
            if (batchSize <= 0) {
                log.error("Invalid batch size configuration: {}", batchSize);
                return;
            }
            if (lockTtlSeconds <= 0) {
                log.error("Invalid lock TTL configuration: {}", lockTtlSeconds);
                return;
            }
            
            int claimedCount = outboxService.claimPendingRows(instanceId, lockTtlSeconds, batchSize);
            
            if (claimedCount == 0) {
                log.debug("No pending outbox records to process");
                return;
            }

            log.info("Claimed {} outbox records for processing", claimedCount);

            List<EventOutbox> claimedRows = outboxService.getClaimedRows(instanceId, lockTtlSeconds);
            
            if (claimedRows == null || claimedRows.isEmpty()) {
                log.warn("No claimed rows retrieved after claiming {} records", claimedCount);
                return;
            }

            log.info("Processing {} claimed outbox records", claimedRows.size());

            int processedCount = 0;
            int failedCount = 0;
            for (EventOutbox outbox : claimedRows) {
                if (outbox == null) {
                    log.warn("Skipping null outbox record");
                    continue;
                }
                try {
                    outboxService.processOutboxRecord(outbox, bigQueryService, maxRetryCount);
                    processedCount++;
                } catch (Exception e) {
                    failedCount++;
                    log.error("Failed to process outbox record: outboxId={}, error={}", 
                        outbox.getId(), e.getMessage(), e);
                }
            }
            
            try {
                bigQueryService.flushBatch();
            } catch (Exception e) {
                log.warn("Error flushing BigQuery batch: {}", e.getMessage());
            }

            log.info("Completed processing: {} succeeded, {} failed out of {} claimed records", 
                processedCount, failedCount, claimedRows.size());

        } catch (IllegalArgumentException e) {
            log.error("Invalid configuration in outbox scheduler: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error in outbox scheduler: {}", e.getMessage(), e);
        }
    }
}
