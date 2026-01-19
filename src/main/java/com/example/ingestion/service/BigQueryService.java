package com.example.ingestion.service;

import com.example.ingestion.dto.BigQueryStatusResponse;
import com.example.ingestion.entity.EventInbox;
import com.example.ingestion.exception.BigQueryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
public class BigQueryService {

    private final BigQuery bigQuery;
    private final Storage storage;
    private final ObjectMapper objectMapper;
    private final Counter bigqueryInsertSuccessTotal;
    private final Counter bigqueryInsertFailureTotal;

    @Value("${bigquery.dataset}")
    private String datasetName;

    @Value("${bigquery.table}")
    private String tableName;

    @Value("${bigquery.project-id}")
    private String projectId;

    @Value("${bigquery.use-batch-loading:true}")
    private boolean useBatchLoading;

    @Value("${bigquery.batch-size:100}")
    private int batchSize;

    @Value("${bigquery.skip-if-unavailable:false}")
    private boolean skipIfUnavailable;

    @Value("${bigquery.use-load-jobs:true}")
    private boolean useLoadJobs;

    @Value("${bigquery.gcs-bucket:}")
    private String gcsBucket;

    @Value("${bigquery.gcs-temp-prefix:temp/events/}")
    private String gcsTempPrefix;

    @Value("${bigquery.cleanup-temp-files:true}")
    private boolean cleanupTempFiles;

    // Batch collection for batch loading mode
    private final Queue<EventInbox> batchQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock batchLock = new ReentrantLock();
    
    // Flag to track if BigQuery is unavailable (free tier detected)
    private volatile boolean bigQueryUnavailable = false;

    public BigQueryService(BigQuery bigQuery,
                          Storage storage,
                          ObjectMapper objectMapper,
                          Counter bigqueryInsertSuccessTotal,
                          Counter bigqueryInsertFailureTotal) {
        this.bigQuery = bigQuery;
        this.storage = storage;
        this.objectMapper = objectMapper;
        this.bigqueryInsertSuccessTotal = bigqueryInsertSuccessTotal;
        this.bigqueryInsertFailureTotal = bigqueryInsertFailureTotal;
    }

    @PostConstruct
    public void initialize() {
        log.info("Initializing BigQuery service - ensuring dataset and table exist...");
        if (useLoadJobs) {
            log.info("✅ BigQuery loading mode: LOAD JOBS with Cloud Storage (FREE TIER COMPATIBLE - no billing required)");
            try {
                ensureGcsBucketExists();
                log.info("Using Cloud Storage bucket: {}", gcsBucket);
            } catch (Exception e) {
                log.error("❌ Failed to initialize GCS bucket: {}", e.getMessage());
                log.error("   Error details: {}", e.getClass().getSimpleName());
                if (e.getCause() != null) {
                    log.error("   Caused by: {}", e.getCause().getMessage());
                }
                log.error("   LOAD jobs will fail until this is resolved.");
                log.error("   Possible causes:");
                log.error("   1. Service account lacks Storage Admin role");
                log.error("   2. Invalid bucket name or bucket name already taken");
                log.error("   3. Network/authentication issues with GCP");
                log.error("   To continue without BigQuery, set: bigquery.skip-if-unavailable=true");
                
                // Don't fail startup - mark as unavailable and try again later
                log.warn("   Continuing startup. Will retry GCS bucket creation on first use.");
                bigQueryUnavailable = true;
            }
        } else if (useBatchLoading) {
            log.info("BigQuery loading mode: BATCH LOADING (streaming API - requires billing)");
        } else {
            log.info("BigQuery loading mode: STREAMING (requires paid account)");
        }
        if (skipIfUnavailable) {
            log.warn("⚠️  BigQuery skip-if-unavailable is enabled. Events will be marked DONE without BigQuery insertion if billing is not enabled.");
        }
        try {
            ensureTableExists();
        } catch (Exception e) {
            log.error("❌ Failed to ensure BigQuery table exists: {}", e.getMessage());
            log.error("   Error details: {}", e.getClass().getSimpleName());
            if (e.getCause() != null) {
                log.error("   Caused by: {}", e.getCause().getMessage());
            }
            if (!skipIfUnavailable) {
                log.warn("   Continuing startup. Table will be created on first use.");
                bigQueryUnavailable = true;
            } else {
                log.warn("   Continuing with skip-if-unavailable enabled. BigQuery operations will be skipped.");
                bigQueryUnavailable = true;
            }
        }
    }

    public void streamInsert(EventInbox eventInbox) {
        if (eventInbox == null) {
            throw new IllegalArgumentException("EventInbox cannot be null");
        }
        if (eventInbox.getTenantId() == null || eventInbox.getTenantId().trim().isEmpty()) {
            throw new IllegalArgumentException("EventInbox tenantId cannot be null or empty");
        }
        if (eventInbox.getEventId() == null || eventInbox.getEventId().trim().isEmpty()) {
            throw new IllegalArgumentException("EventInbox eventId cannot be null or empty");
        }
        
        // Check if BigQuery is unavailable and we should skip
        if (bigQueryUnavailable && skipIfUnavailable) {
            log.warn("⚠️ Skipping BigQuery insertion for event {} (BigQuery unavailable, skip-if-unavailable=true). " +
                    "Event will be marked DONE without BigQuery insertion.", eventInbox.getEventId());
            bigqueryInsertSuccessTotal.increment(); // Count as success since we're intentionally skipping
            return;
        }
        
        // If BigQuery is unavailable but skip is false, try to recover
        if (bigQueryUnavailable && !skipIfUnavailable) {
            log.info("BigQuery was marked unavailable, attempting to recover for event: {}", eventInbox.getEventId());
            // Reset flag and try again - the batchLoadWithLoadJob will retry bucket creation
            bigQueryUnavailable = false;
        }
        
        try {
            if (useLoadJobs) {
                // Use LOAD jobs via Cloud Storage (free tier compatible)
                addToBatch(eventInbox);
            } else if (useBatchLoading) {
                // Use batch streaming (still requires paid account)
                addToBatch(eventInbox);
            } else {
                // Use streaming insert (requires paid account)
                streamInsertDirect(eventInbox);
            }
        } catch (BigQueryException e) {
            // Check if it's a free tier error
            if (isFreeTierError(e) && skipIfUnavailable) {
                log.warn("BigQuery free tier detected. Skipping BigQuery operations for remaining events. " +
                         "Enable billing on your GCP project to use BigQuery. Event: {}", eventInbox.getEventId());
                bigQueryUnavailable = true;
                bigqueryInsertSuccessTotal.increment(); // Count as success since we're gracefully skipping
                return;
            }
            // Re-throw if we shouldn't skip or it's a different error
            throw e;
        }
    }
    
    private boolean isFreeTierError(BigQueryException e) {
        Throwable cause = e.getCause();
        if (cause instanceof com.google.cloud.bigquery.BigQueryException) {
            com.google.cloud.bigquery.BigQueryException bqEx = (com.google.cloud.bigquery.BigQueryException) cause;
            return bqEx.getCode() == 403 && 
                   bqEx.getMessage() != null && 
                   bqEx.getMessage().contains("Streaming insert is not allowed in the free tier");
        }
        return e.getMessage() != null && 
               e.getMessage().contains("Streaming insert is not allowed in the free tier");
    }

    private void addToBatch(EventInbox eventInbox) {
        if (eventInbox == null) {
            log.warn("Attempted to add null event to batch");
            return;
        }
        
        batchQueue.offer(eventInbox);
        
        if (batchQueue.size() >= batchSize) {
            flushBatch();
        }
    }

    public void flushBatch() {
        if (batchQueue.isEmpty()) {
            return;
        }

        List<EventInbox> eventsToLoad = new ArrayList<>();
        batchLock.lock();
        try {
            // Drain the queue - flush ALL pending events, not just up to batchSize
            while (!batchQueue.isEmpty()) {
                EventInbox event = batchQueue.poll();
                if (event != null) {
                    eventsToLoad.add(event);
                }
            }
        } finally {
            batchLock.unlock();
        }

        if (eventsToLoad.isEmpty()) {
            return;
        }

        // Check if BigQuery is unavailable before attempting load
        if (bigQueryUnavailable && skipIfUnavailable) {
            log.warn("Skipping BigQuery batch load for {} events (BigQuery unavailable, skip-if-unavailable=true)", 
                eventsToLoad.size());
            bigqueryInsertSuccessTotal.increment(eventsToLoad.size()); // Count as success since we're gracefully skipping
            return;
        }

        try {
            batchLoad(eventsToLoad);
            bigqueryInsertSuccessTotal.increment(eventsToLoad.size());
            log.info("Successfully batch loaded {} events to BigQuery", eventsToLoad.size());
        } catch (Exception e) {
            bigqueryInsertFailureTotal.increment(eventsToLoad.size());
            log.error("Error batch loading {} events to BigQuery: {}", eventsToLoad.size(), e.getMessage(), e);
            
            // If it's a skip scenario, handle gracefully
            if (skipIfUnavailable) {
                log.warn("BigQuery batch load failed, but skip-if-unavailable=true. Marking events as processed.");
                bigQueryUnavailable = true;
                bigqueryInsertSuccessTotal.increment(eventsToLoad.size());
                return;
            }
            
            throw new BigQueryException("Failed to batch load events to BigQuery", e);
        }
    }

    private void batchLoad(List<EventInbox> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("Events list cannot be null or empty");
        }
        
        // Use LOAD jobs with Cloud Storage if enabled (free tier compatible)
        if (useLoadJobs) {
            batchLoadWithLoadJob(events);
            return;
        }
        
        // Otherwise use streaming insert (requires billing)
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        
        try {
            // Convert events to BigQuery rows
            List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
            for (EventInbox eventInbox : events) {
                if (eventInbox == null) {
                    log.warn("Skipping null event in batch");
                    continue;
                }
                try {
                    Map<String, Object> rowContent = convertToBigQueryRow(eventInbox);
                    rows.add(InsertAllRequest.RowToInsert.of(UUID.randomUUID().toString(), rowContent));
                } catch (Exception e) {
                    log.error("Error converting event to BigQuery row: eventId={}, error={}", 
                        eventInbox.getEventId(), e.getMessage(), e);
                    throw new BigQueryException("Failed to convert event to BigQuery row: " + eventInbox.getEventId(), e);
                }
            }
            
            if (rows.isEmpty()) {
                log.warn("No valid rows to insert after filtering");
                return;
            }
            
            // Use insertAll with multiple rows in a single batch (streaming API)
            InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(tableId);
            for (InsertAllRequest.RowToInsert row : rows) {
                requestBuilder.addRow(row.getId(), row.getContent());
            }
            InsertAllRequest request = requestBuilder.setSkipInvalidRows(false).build();
            
            // Execute batch insert
            InsertAllResponse response = bigQuery.insertAll(request);
            
            // Handle errors
            if (response.hasErrors()) {
                Map<Long, java.util.List<BigQueryError>> errors = response.getInsertErrors();
                log.error("Errors occurred while batch loading {} events: {} errors", events.size(), errors.size());
                
                // Log detailed errors
                errors.forEach((index, errorList) -> {
                    errorList.forEach(error -> {
                        log.error("Row {} error in batch: {} - {}", index, error.getReason(), error.getMessage());
                    });
                });
                
                throw new BigQueryException(
                    String.format("Failed to batch load %d events: %s errors", events.size(), errors.size()));
            }
            
            log.info("Successfully batch loaded {} events to BigQuery", events.size());
            
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            // If free tier error, handle gracefully if skip is enabled
            if (e.getCode() == 403 && e.getMessage() != null && 
                e.getMessage().contains("Streaming insert is not allowed in the free tier")) {
                String errorMsg = String.format(
                    "BigQuery batch loading failed: Free tier does not support streaming inserts (even batched). " +
                    "Please enable billing on your GCP project or set bigquery.use-load-jobs=true. Error: %s", e.getMessage());
                
                if (skipIfUnavailable) {
                    log.warn("BigQuery free tier detected during batch load. Skipping BigQuery operations. " +
                             "Set bigquery.use-load-jobs=true to use LOAD jobs (free tier compatible).");
                    bigQueryUnavailable = true;
                    bigqueryInsertSuccessTotal.increment(events.size()); // Count as success
                    return; // Gracefully skip
                }
                
                log.error(errorMsg);
                throw new BigQueryException(errorMsg, e);
            }
            throw new BigQueryException("Failed to batch load events to BigQuery: " + e.getMessage(), e);
        } catch (Exception e) {
            // Check if the wrapped exception is a free tier error
            if (e instanceof BigQueryException && isFreeTierError((BigQueryException) e) && skipIfUnavailable) {
                log.warn("BigQuery free tier detected during batch load. Skipping BigQuery operations.");
                bigQueryUnavailable = true;
                bigqueryInsertSuccessTotal.increment(events.size());
                return;
            }
            throw new BigQueryException("Failed to batch load events to BigQuery", e);
        }
    }

    /**
     * Load events to BigQuery using LOAD jobs with Cloud Storage (free tier compatible)
     * This method works without billing enabled
     */
    private void batchLoadWithLoadJob(List<EventInbox> events) {
        // Always ensure bucket exists before attempting load
        try {
            log.debug("Ensuring GCS bucket exists before LOAD job...");
            ensureGcsBucketExists();
            bigQueryUnavailable = false; // Reset if successful
            log.debug("GCS bucket verified/created: {}", gcsBucket);
        } catch (Exception e) {
            log.error("Failed to ensure GCS bucket exists: {}", e.getMessage(), e);
            if (skipIfUnavailable) {
                log.warn("Skipping BigQuery load due to unavailable GCS bucket (skip-if-unavailable=true)");
                return; // Skip the load operation gracefully
            } else {
                throw new BigQueryException("GCS bucket is not available. Please check configuration and permissions. " +
                    "Error: " + e.getMessage() + ". Set bigquery.skip-if-unavailable=true to skip BigQuery operations.", e);
            }
        }
        
        String gcsFileName = gcsTempPrefix + "events_" + UUID.randomUUID().toString() + "_" + System.currentTimeMillis() + ".json";
        String gcsUri = "gs://" + gcsBucket + "/" + gcsFileName;
        BlobId blobId = null;
        
        try {
            // Step 1: Convert events to JSON Lines format (newline-delimited JSON)
            StringBuilder jsonLines = new StringBuilder();
            for (EventInbox eventInbox : events) {
                if (eventInbox == null) {
                    continue;
                }
                try {
                    Map<String, Object> rowContent = convertToBigQueryRow(eventInbox);
                    jsonLines.append(objectMapper.writeValueAsString(rowContent)).append("\n");
                } catch (Exception e) {
                    log.error("Error converting event to JSON: eventId={}, error={}", 
                        eventInbox.getEventId(), e.getMessage(), e);
                    // Continue with other events
                }
            }
            
            if (jsonLines.length() == 0) {
                log.warn("No valid events to load after conversion");
                return;
            }
            
            // Step 2: Upload JSON to Cloud Storage
            byte[] jsonData = jsonLines.toString().getBytes(StandardCharsets.UTF_8);
            blobId = BlobId.of(gcsBucket, gcsFileName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType("application/json")
                    .build();
            
            storage.create(blobInfo, jsonData);
            log.debug("Uploaded {} events to Cloud Storage: {}", events.size(), gcsUri);
            
            // Step 3: Create LOAD job to load from Cloud Storage to BigQuery
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            
            // Get table schema for LOAD job
            Table table = bigQuery.getTable(tableId);
            if (table == null) {
                throw new BigQueryException("Table does not exist: " + tableId);
            }
            
            // Create LOAD job configuration
            LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, gcsUri)
                    .setFormatOptions(FormatOptions.json())
                    .setWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .setAutodetect(false) // Use existing schema
                    .build();
            
            // Create and run the job
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job loadJob = bigQuery.create(JobInfo.newBuilder(loadConfig).setJobId(jobId).build());
            
            log.info("Created LOAD job {} to load {} events from {}", jobId.getJob(), events.size(), gcsUri);
            
            // Step 4: Wait for job to complete
            loadJob = loadJob.waitFor();
            
            if (loadJob == null) {
                throw new BigQueryException("LOAD job no longer exists: " + jobId);
            }
            
            if (loadJob.getStatus().getError() != null) {
                String errorMsg = "LOAD job failed: " + loadJob.getStatus().getError();
                log.error(errorMsg);
                throw new BigQueryException(errorMsg);
            }
            
            // Step 5: Cleanup - delete temporary file from Cloud Storage
            if (cleanupTempFiles) {
                try {
                    storage.delete(blobId);
                    log.debug("Cleaned up temporary file from Cloud Storage: {}", gcsUri);
                } catch (Exception e) {
                    log.warn("Failed to cleanup temporary file from Cloud Storage: {}. Error: {}", gcsUri, e.getMessage());
                    // Don't fail the operation if cleanup fails
                }
            }
            
            log.info("Successfully loaded {} events to BigQuery using LOAD job {} (free tier compatible)", 
                events.size(), jobId.getJob());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("LOAD job interrupted: {}", e.getMessage(), e);
            throw new BigQueryException("LOAD job interrupted", e);
        } catch (Exception e) {
            log.error("Error loading events via LOAD job: {}", e.getMessage(), e);
            // Try to cleanup on error
            if (blobId != null && cleanupTempFiles) {
                try {
                    storage.delete(blobId);
                    log.debug("Cleaned up temporary file after error: {}", gcsUri);
                } catch (Exception cleanupEx) {
                    log.warn("Failed to cleanup temporary file after error: {}", cleanupEx.getMessage());
                }
            }
            throw new BigQueryException("Failed to load events via LOAD job: " + e.getMessage(), e);
        }
    }

    private void streamInsertDirect(EventInbox eventInbox) {
        try {
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            
            Map<String, Object> rowContent = convertToBigQueryRow(eventInbox);
            
            // Create insert request
            InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(tableId);
            requestBuilder.addRow(UUID.randomUUID().toString(), rowContent);
            InsertAllRequest request = requestBuilder.build();

            // Execute insert
            InsertAllResponse response = bigQuery.insertAll(request);

            // Handle partial failures
            if (response.hasErrors()) {
                Map<Long, java.util.List<BigQueryError>> errors = response.getInsertErrors();
                log.error("Errors occurred while inserting to BigQuery for event {}: {} errors out of {} rows", 
                    eventInbox.getEventId(), errors.size(), request.getRows().size());
                
                // Log detailed errors
                errors.forEach((index, errorList) -> {
                    errorList.forEach(error -> {
                        log.error("Row {} error for event {}: {} - {}", 
                            index, eventInbox.getEventId(), error.getReason(), error.getMessage());
                    });
                });
                
                bigqueryInsertFailureTotal.increment();
                throw new BigQueryException(
                    String.format("Failed to insert event %s to BigQuery: %s", eventInbox.getEventId(), errors));
            }

            bigqueryInsertSuccessTotal.increment();
            log.debug("Successfully inserted event {} to BigQuery", eventInbox.getEventId());
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            // Check if it's the free tier limitation
            if (e.getCode() == 403 && e.getMessage() != null && 
                e.getMessage().contains("Streaming insert is not allowed in the free tier")) {
                bigqueryInsertFailureTotal.increment();
                String errorMsg = String.format(
                    "BigQuery streaming inserts require a paid account. Free tier does not support streaming inserts. " +
                    "Error for event %s: %s. Please set bigquery.use-batch-loading=true to use batch loading instead.",
                    eventInbox.getEventId(), e.getMessage());
                log.error(errorMsg);
                throw new BigQueryException(errorMsg, e);
            }
            // Re-throw other BigQuery exceptions
            bigqueryInsertFailureTotal.increment();
            throw new BigQueryException(
                String.format("Failed to stream insert event %s to BigQuery: %s", eventInbox.getEventId(), e.getMessage()), e);
        } catch (BigQueryException e) {
            throw e;
        } catch (Exception e) {
            bigqueryInsertFailureTotal.increment();
            log.error("Error streaming insert to BigQuery for event {}: {}", eventInbox.getEventId(), e.getMessage(), e);
            throw new BigQueryException(
                String.format("Failed to stream insert event %s to BigQuery", eventInbox.getEventId()), e);
        }
    }

    private Map<String, Object> convertToBigQueryRow(EventInbox eventInbox) {
        if (eventInbox == null) {
            throw new IllegalArgumentException("EventInbox cannot be null");
        }
        if (eventInbox.getEventTs() == null) {
            throw new IllegalArgumentException("EventInbox eventTs cannot be null");
        }
        if (eventInbox.getPayloadJson() == null) {
            throw new IllegalArgumentException("EventInbox payloadJson cannot be null");
        }
        
        try {
            // Convert LocalDateTime to microseconds since epoch for BigQuery TIMESTAMP
            long eventTsMicros = eventInbox.getEventTs().atOffset(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
            long ingestedAtMicros = Instant.now().toEpochMilli() * 1000;
            
            // Extract event_date for partitioning (date part of event_ts)
            String eventDate = eventInbox.getEventTs().toLocalDate().toString();
            
            // Calculate payload hash
            String payloadHash = calculateSHA256(eventInbox.getPayloadJson());
            
            // Create structured row content
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("tenant_id", eventInbox.getTenantId());
            rowContent.put("event_id", eventInbox.getEventId());
            rowContent.put("event_type", eventInbox.getEventType());
            rowContent.put("event_ts", eventTsMicros);
            rowContent.put("event_date", eventDate);
            rowContent.put("source", eventInbox.getSource() != null ? eventInbox.getSource() : "");
            rowContent.put("payload_hash", payloadHash);
            rowContent.put("payload_json", eventInbox.getPayloadJson());
            rowContent.put("ingested_at", ingestedAtMicros);
            
            return rowContent;
        } catch (Exception e) {
            log.error("Error converting event to BigQuery row: eventId={}", eventInbox.getEventId(), e);
            throw new BigQueryException("Failed to convert event to BigQuery row: " + eventInbox.getEventId(), e);
        }
    }

    public void ensureTableExists() {
        try {
            // First ensure dataset exists
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            if (dataset == null) {
                log.info("Dataset {}.{} does not exist, creating...", projectId, datasetName);
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).build();
                bigQuery.create(datasetInfo);
                log.info("Created BigQuery dataset: {}.{}", projectId, datasetName);
            } else {
                log.debug("BigQuery dataset {}.{} already exists", projectId, datasetName);
            }

            // Then check and create table if needed
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            Table table = bigQuery.getTable(tableId);

            if (table == null) {
                log.info("Table {}.{}.{} does not exist, creating...", projectId, datasetName, tableName);
                createTable(tableId);
            } else {
                log.debug("BigQuery table {}.{}.{} already exists", projectId, datasetName, tableName);
            }
        } catch (com.google.cloud.bigquery.BigQueryException e) {
            if (e.getCode() == 404) {
                // Dataset or table not found - try to create
                log.info("Dataset or table not found (404), attempting to create...");
                try {
                    ensureDatasetExists();
                    createTable(TableId.of(projectId, datasetName, tableName));
                } catch (Exception createEx) {
                    log.error("Error creating dataset/table after 404: {}", createEx.getMessage(), createEx);
                }
            } else {
                log.warn("Error checking/creating BigQuery dataset/table: {} (Code: {})", e.getMessage(), e.getCode());
            }
        } catch (Exception e) {
            log.warn("Error checking/creating BigQuery dataset/table: {}", e.getMessage());
        }
    }

    private void ensureDatasetExists() {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            if (dataset == null) {
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).build();
                bigQuery.create(datasetInfo);
                log.info("Created BigQuery dataset: {}.{}", projectId, datasetName);
            }
        } catch (Exception e) {
            log.error("Error creating BigQuery dataset: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create BigQuery dataset", e);
        }
    }

    /**
     * Ensures the Cloud Storage bucket exists for LOAD jobs
     * Auto-creates bucket if not provided or doesn't exist
     */
    private void ensureGcsBucketExists() {
        try {
            // Auto-generate bucket name if not provided
            if (gcsBucket == null || gcsBucket.trim().isEmpty()) {
                // Generate a valid bucket name: project-id-bigquery-load-temp (lowercase, no special chars)
                String autoBucketName = (projectId + "-bigquery-load-temp").toLowerCase()
                        .replaceAll("[^a-z0-9-]", "-")
                        .replaceAll("-+", "-")
                        .replaceAll("^-|-$", "");
                gcsBucket = autoBucketName;
                log.info("Auto-generated GCS bucket name: {}", gcsBucket);
            }

            // Check if bucket exists by trying to get it
            com.google.cloud.storage.Bucket bucket = null;
            try {
                bucket = storage.get(gcsBucket);
            } catch (com.google.cloud.storage.StorageException e) {
                if (e.getCode() == 404) {
                    // Bucket doesn't exist - will create below
                    log.info("GCS bucket {} does not exist (404), will create...", gcsBucket);
                } else {
                    // Re-throw other errors
                    throw e;
                }
            }
            
            if (bucket == null) {
                log.info("Creating GCS bucket: {}...", gcsBucket);
                try {
                    bucket = storage.create(
                        com.google.cloud.storage.BucketInfo.newBuilder(gcsBucket)
                            .setLocation("US") // Default location
                            .build()
                    );
                    log.info("✅ Successfully created GCS bucket: {} (for LOAD jobs - free tier compatible)", gcsBucket);
                } catch (com.google.cloud.storage.StorageException e) {
                    if (e.getCode() == 409) {
                        // Bucket already exists (race condition - created by another process)
                        log.info("GCS bucket {} already exists (created by another process)", gcsBucket);
                        // Verify it exists now
                        bucket = storage.get(gcsBucket);
                        if (bucket == null) {
                            throw new IllegalStateException("Bucket creation reported 409 but bucket still not found: " + gcsBucket);
                        }
                    } else {
                        throw e;
                    }
                }
            } else {
                log.debug("GCS bucket {} already exists and is accessible", gcsBucket);
            }
            } catch (com.google.cloud.storage.StorageException e) {
            if (e.getCode() == 403) {
                log.error("❌ Permission denied creating/accessing GCS bucket: {}", gcsBucket);
                log.error("   Your service account needs one of the following:");
                log.error("   1. Storage Admin role (on project) - to create buckets");
                log.error("   2. Storage Object Admin role (on bucket) - if bucket already exists");
                log.error("   See GCS_BUCKET_SETUP.md for detailed instructions.");
                log.error("   Alternative: Manually create the bucket and grant Storage Object Admin role.");
                throw new IllegalStateException("Insufficient permissions for GCS bucket: " + gcsBucket + 
                    ". Service account needs Storage Admin role (to create) or Storage Object Admin role (if bucket exists). " +
                    "See GCS_BUCKET_SETUP.md for setup instructions.", e);
            } else if (e.getCode() == 404) {
                log.error("❌ GCS bucket {} not found after creation attempt.", gcsBucket);
                log.error("   Please verify:");
                log.error("   1. Bucket name is correct");
                log.error("   2. Service account has Storage Admin role to create buckets");
                log.error("   3. Or manually create the bucket and grant Storage Object Admin role");
                log.error("   See GCS_BUCKET_SETUP.md for detailed instructions.");
                throw new IllegalStateException("GCS bucket not found: " + gcsBucket + 
                    ". See GCS_BUCKET_SETUP.md for setup instructions.", e);
            } else {
                log.error("Error checking/creating GCS bucket: {} (Code: {})", e.getMessage(), e.getCode(), e);
                throw new IllegalStateException("Failed to ensure GCS bucket exists: " + gcsBucket + 
                    " (Error code: " + e.getCode() + "). See GCS_BUCKET_SETUP.md for troubleshooting.", e);
            }
        } catch (Exception e) {
            log.error("Error ensuring GCS bucket exists: {}", e.getMessage(), e);
            throw new IllegalStateException("Failed to ensure GCS bucket exists: " + gcsBucket, e);
        }
    }

    private void createTable(TableId tableId) {
        try {
            // Ensure dataset exists first (in case it wasn't created)
            ensureDatasetExists();

            Schema schema = Schema.of(
                Field.newBuilder("tenant_id", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("event_id", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("event_type", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("event_ts", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("event_date", StandardSQLTypeName.DATE).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("source", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("payload_hash", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("payload_json", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
                Field.newBuilder("ingested_at", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build()
            );

            // Create table definition with partitioning and clustering
            TimePartitioning timePartitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                    .setField("event_date")
                    .build();

            Clustering clustering = Clustering.newBuilder()
                    .setFields(java.util.Arrays.asList("tenant_id"))
                    .build();

            StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(schema)
                    .setTimePartitioning(timePartitioning)
                    .setClustering(clustering)
                    .build();

            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigQuery.create(tableInfo);
            log.info("Created BigQuery table {}.{}.{} with partitioning and clustering", projectId, datasetName, tableName);
        } catch (Exception e) {
            log.error("Error creating BigQuery table: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create BigQuery table", e);
        }
    }

    /**
     * Verify if an event exists in BigQuery
     * @param tenantId Tenant ID
     * @param eventId Event ID
     * @return true if event exists in BigQuery, false otherwise
     */
    public boolean verifyEventExists(String tenantId, String eventId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID cannot be null or empty");
        }
        if (eventId == null || eventId.trim().isEmpty()) {
            throw new IllegalArgumentException("Event ID cannot be null or empty");
        }
        
        if (bigQueryUnavailable && skipIfUnavailable) {
            log.debug("BigQuery unavailable, cannot verify event: tenantId={}, eventId={}", tenantId, eventId);
            return false;
        }
        
        try {
            // Query to check if event exists
            String query = String.format(
                "SELECT COUNT(*) as count " +
                "FROM `%s.%s.%s` " +
                "WHERE tenant_id = @tenantId AND event_id = @eventId",
                projectId, datasetName, tableName);
            
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                    .addNamedParameter("tenantId", QueryParameterValue.string(tenantId))
                    .addNamedParameter("eventId", QueryParameterValue.string(eventId))
                    .build();
            
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            
            // Wait for query to complete
            queryJob = queryJob.waitFor();
            
            if (queryJob == null) {
                log.error("Job no longer exists for verification query");
                return false;
            }
            
            if (queryJob.getStatus().getError() != null) {
                log.error("Error executing verification query: {}", queryJob.getStatus().getError());
                return false;
            }
            
            // Get results
            TableResult result = queryJob.getQueryResults();
            if (result.getTotalRows() > 0) {
                for (FieldValueList row : result.iterateAll()) {
                    long count = row.get(0).getLongValue();
                    return count > 0;
                }
            }
            
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Verification query interrupted: tenantId={}, eventId={}", tenantId, eventId, e);
            return false;
        } catch (Exception e) {
            log.error("Error verifying event in BigQuery: tenantId={}, eventId={}", tenantId, eventId, e);
            return false;
        }
    }
    
    /**
     * Get count of events in BigQuery for a given tenant
     * @param tenantId Tenant ID (empty string for all tenants)
     * @return Count of events
     */
    public long getEventCount(String tenantId) {
        if (bigQueryUnavailable && skipIfUnavailable) {
            log.debug("BigQuery unavailable, cannot get event count");
            return 0;
        }
        
        try {
            String query;
            if (tenantId == null || tenantId.trim().isEmpty()) {
                query = String.format("SELECT COUNT(*) as count FROM `%s.%s.%s`", 
                    projectId, datasetName, tableName);
            } else {
                query = String.format(
                    "SELECT COUNT(*) as count FROM `%s.%s.%s` WHERE tenant_id = @tenantId",
                    projectId, datasetName, tableName);
            }
            
            QueryJobConfiguration.Builder queryBuilder = QueryJobConfiguration.newBuilder(query);
            if (tenantId != null && !tenantId.trim().isEmpty()) {
                queryBuilder.addNamedParameter("tenantId", QueryParameterValue.string(tenantId));
            }
            QueryJobConfiguration queryConfig = queryBuilder.build();
            
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            
            queryJob = queryJob.waitFor();
            
            if (queryJob == null || queryJob.getStatus().getError() != null) {
                log.error("Error executing count query: {}", 
                    queryJob != null ? queryJob.getStatus().getError() : "Job is null");
                return 0;
            }
            
            TableResult result = queryJob.getQueryResults();
            if (result.getTotalRows() > 0) {
                for (FieldValueList row : result.iterateAll()) {
                    return row.get(0).getLongValue();
                }
            }
            
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Count query interrupted: tenantId={}", tenantId, e);
            return 0;
        } catch (Exception e) {
            log.error("Error getting event count from BigQuery: tenantId={}", tenantId, e);
            return 0;
        }
    }

    /**
     * Get BigQuery service status
     * @return Status information including availability and configuration
     */
    public BigQueryStatusResponse getStatus() {
        boolean available = !(bigQueryUnavailable && skipIfUnavailable);
        String message;
        
        if (bigQueryUnavailable && skipIfUnavailable) {
            message = "BigQuery operations are being skipped due to free tier limitation. " +
                      "Enable billing on your GCP project to send data to BigQuery. " +
                      "Events are currently marked as DONE without BigQuery insertion.";
        } else if (bigQueryUnavailable) {
            message = "BigQuery is unavailable but skip-if-unavailable is false. Events will fail.";
        } else {
            message = "BigQuery is available and operational.";
        }
        
        return new BigQueryStatusResponse(
            available,
            skipIfUnavailable,
            bigQueryUnavailable,
            projectId,
            datasetName,
            tableName,
            message
        );
    }

    private String calculateSHA256(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            log.error("SHA-256 algorithm not available", e);
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
