package com.example.ingestion.service;

import com.example.ingestion.dto.EventRequest;
import com.example.ingestion.dto.EventResponse;
import com.example.ingestion.entity.EventInbox;
import com.example.ingestion.entity.EventOutbox;
import com.example.ingestion.entity.EventOutbox.OutboxStatus;
import com.example.ingestion.exception.EventNotFoundException;
import com.example.ingestion.repository.EventInboxRepository;
import com.example.ingestion.repository.EventOutboxRepository;
import com.example.ingestion.util.Constants;
import com.example.ingestion.validator.EventValidator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EventService {

    private final EventInboxRepository eventInboxRepository;
    private final EventOutboxRepository eventOutboxRepository;
    private final ObjectMapper objectMapper;
    private final Counter eventsIngestedTotal;
    private final EventValidator eventValidator;

    public EventService(EventInboxRepository eventInboxRepository, 
                       EventOutboxRepository eventOutboxRepository,
                       ObjectMapper objectMapper,
                       Counter eventsIngestedTotal,
                       EventValidator eventValidator) {
        this.eventInboxRepository = eventInboxRepository;
        this.eventOutboxRepository = eventOutboxRepository;
        this.objectMapper = objectMapper;
        this.eventsIngestedTotal = eventsIngestedTotal;
        this.eventValidator = eventValidator;
    }

    @Transactional
    public EventResponse ingestEvent(EventRequest request) {
        eventValidator.validate(request);
        
        // Check idempotency
        if (eventInboxRepository.existsByTenantIdAndEventId(request.tenantId(), request.eventId())) {
            log.warn("Event already exists: tenantId={}, eventId={}", request.tenantId(), request.eventId());
            EventInbox existingEvent = eventInboxRepository.findByTenantIdAndEventId(
                request.tenantId(), request.eventId()).orElseThrow();
            EventOutbox outbox = eventOutboxRepository.findByEventId(request.eventId()).orElse(null);
            return toResponse(existingEvent, outbox);
        }

        // Create event
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(request.payload());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize payload for event: tenantId={}, eventId={}", 
                request.tenantId(), request.eventId(), e);
            throw new IllegalArgumentException(Constants.ErrorMessages.INVALID_PAYLOAD_FORMAT, e);
        }

        String checksum = calculateChecksum(payloadJson);

        EventInbox eventInbox = EventInbox.builder()
                .tenantId(request.tenantId())
                .eventId(request.eventId())
                .eventType(request.eventType())
                .eventTs(request.eventTs())
                .payloadJson(payloadJson)
                .source(request.source())
                .checksum(checksum)
                .build();
        try {
            eventInbox = eventInboxRepository.save(eventInbox);
        } catch (DataAccessException e) {
            log.error("Database error saving event: tenantId={}, eventId={}", 
                request.tenantId(), request.eventId(), e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
        
        MDC.put("tenantId", eventInbox.getTenantId());
        MDC.put("eventId", eventInbox.getEventId());
        
        log.info("Event saved: tenantId={}, eventId={}", eventInbox.getTenantId(), eventInbox.getEventId());

        // Create outbox entry (only if not already exists)
        EventOutbox outbox = null;
        try {
            if (!eventOutboxRepository.existsByEventId(eventInbox.getEventId())) {
                outbox = EventOutbox.builder()
                        .tenantId(eventInbox.getTenantId())
                        .eventId(eventInbox.getEventId())
                        .status(OutboxStatus.PENDING)
                        .retryCount(0)
                        .nextRetryAt(LocalDateTime.now())
                        .build();
                outbox = eventOutboxRepository.save(outbox);
                MDC.put("outboxId", String.valueOf(outbox.getId()));
                log.info("Outbox entry created: eventId={}, outboxId={}", eventInbox.getEventId(), outbox.getId());
            } else {
                outbox = eventOutboxRepository.findByEventId(eventInbox.getEventId()).orElse(null);
                if (outbox != null) {
                    MDC.put("outboxId", String.valueOf(outbox.getId()));
                }
            }
        } catch (DataAccessException e) {
            log.error("Database error saving outbox: tenantId={}, eventId={}", 
                eventInbox.getTenantId(), eventInbox.getEventId(), e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }

        eventsIngestedTotal.increment();
        
        return toResponse(eventInbox, outbox);
    }

    public List<EventResponse> ingestEvents(List<EventRequest> requests) {
        eventValidator.validateBulkRequest(requests);

        return requests.stream()
                .map(this::ingestEventInNewTransaction)
                .collect(Collectors.toList());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public EventResponse ingestEventInNewTransaction(EventRequest request) {
        return ingestEvent(request);
    }

    public EventResponse getEvent(String tenantId, String eventId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new IllegalArgumentException("Tenant ID cannot be null or empty");
        }
        if (eventId == null || eventId.trim().isEmpty()) {
            throw new IllegalArgumentException("Event ID cannot be null or empty");
        }
        
        EventInbox eventInbox;
        try {
            eventInbox = eventInboxRepository.findByTenantIdAndEventId(tenantId, eventId)
                    .orElseThrow(() -> new EventNotFoundException(tenantId, eventId));
        } catch (DataAccessException e) {
            log.error("Database error retrieving event: tenantId={}, eventId={}", tenantId, eventId, e);
            throw new IllegalStateException(Constants.ErrorMessages.DATABASE_ERROR, e);
        }
        
        EventOutbox outbox = eventOutboxRepository.findByEventId(eventId).orElse(null);
        return toResponse(eventInbox, outbox);
    }

    private EventResponse toResponse(EventInbox eventInbox, EventOutbox outbox) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(
                eventInbox.getPayloadJson(), Map.class);

            return new EventResponse(
                    eventInbox.getTenantId(),
                    eventInbox.getEventId(),
                    eventInbox.getEventType(),
                    eventInbox.getEventTs(),
                    payload,
                    eventInbox.getSource(),
                    outbox != null ? outbox.getStatus() : null,
                    eventInbox.getCreatedAt()
            );
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payload for event: tenantId={}, eventId={}", 
                eventInbox.getTenantId(), eventInbox.getEventId(), e);
            throw new IllegalStateException("Error converting event to response", e);
        }
    }

    private String calculateChecksum(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            log.warn("SHA-256 not available, skipping checksum calculation", e);
            return null;
        }
    }
}
