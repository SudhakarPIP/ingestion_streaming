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
            // Step 1: Claim rows atomically
            int claimedCount = outboxService.claimPendingRows(instanceId, lockTtlSeconds, batchSize);
            
            if (claimedCount == 0) {
                log.debug("No pending outbox records to process");
                return;
            }

            log.info("Claimed {} outbox records for processing", claimedCount);

            // Step 2: Fetch claimed rows
            List<EventOutbox> claimedRows = outboxService.getClaimedRows(instanceId, lockTtlSeconds);
            
            log.info("Processing {} claimed outbox records", claimedRows.size());

            // Step 3: Process each claimed row
            for (EventOutbox outbox : claimedRows) {
                outboxService.processOutboxRecord(outbox, bigQueryService, maxRetryCount);
            }

            log.info("Completed processing batch of {} outbox records", claimedRows.size());

        } catch (Exception e) {
            log.error("Error in outbox scheduler: {}", e.getMessage(), e);
        }
    }
}
