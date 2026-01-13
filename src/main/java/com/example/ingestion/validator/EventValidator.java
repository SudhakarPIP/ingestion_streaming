package com.example.ingestion.validator;

import com.example.ingestion.dto.EventRequest;
import com.example.ingestion.exception.ValidationException;
import com.example.ingestion.util.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class EventValidator {

    private final ObjectMapper objectMapper;

    public EventValidator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void validate(EventRequest request) {
        validateTenantId(request.tenantId());
        validateEventId(request.eventId());
        validateEventType(request.eventType());
        validateEventTimestamp(request.eventTs());
        validatePayload(request.payload());
    }

    public void validateBulkRequest(java.util.List<EventRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            throw new ValidationException("Bulk request cannot be empty");
        }
        
        if (requests.size() > Constants.Limits.MAX_BULK_EVENTS) {
            throw new ValidationException(
                String.format("Bulk request exceeds maximum size of %d events", Constants.Limits.MAX_BULK_EVENTS));
        }
        
        requests.forEach(this::validate);
    }

    private void validateTenantId(String tenantId) {
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new ValidationException(Constants.ErrorMessages.MISSING_REQUIRED_FIELD + ": tenantId");
        }
        
        if (tenantId.length() < Constants.Limits.MIN_TENANT_ID_LENGTH || 
            tenantId.length() > Constants.Validation.MAX_TENANT_ID_LENGTH) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_TENANT_ID);
        }
    }

    private void validateEventId(String eventId) {
        if (eventId == null || eventId.trim().isEmpty()) {
            throw new ValidationException(Constants.ErrorMessages.MISSING_REQUIRED_FIELD + ": eventId");
        }
        
        if (eventId.length() < Constants.Limits.MIN_EVENT_ID_LENGTH || 
            eventId.length() > Constants.Validation.MAX_EVENT_ID_LENGTH) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_EVENT_ID);
        }
    }

    private void validateEventType(String eventType) {
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new ValidationException(Constants.ErrorMessages.MISSING_REQUIRED_FIELD + ": eventType");
        }
        
        if (eventType.length() > Constants.Validation.MAX_EVENT_TYPE_LENGTH) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_EVENT_TYPE);
        }
    }

    private void validateEventTimestamp(LocalDateTime eventTs) {
        if (eventTs == null) {
            throw new ValidationException(Constants.ErrorMessages.MISSING_REQUIRED_FIELD + ": eventTs");
        }
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime maxPastDate = now.minusYears(10);
        LocalDateTime maxFutureDate = now.plusDays(1);
        
        if (eventTs.isBefore(maxPastDate)) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_EVENT_TIMESTAMP + ": date is too far in the past");
        }
        
        if (eventTs.isAfter(maxFutureDate)) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_EVENT_TIMESTAMP + ": date is too far in the future");
        }
    }

    private void validatePayload(java.util.Map<String, Object> payload) {
        if (payload == null) {
            throw new ValidationException(Constants.ErrorMessages.MISSING_REQUIRED_FIELD + ": payload");
        }
        
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            int payloadSize = payloadJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            
            if (payloadSize > Constants.Limits.MAX_PAYLOAD_SIZE_BYTES) {
                throw new ValidationException(
                    Constants.ErrorMessages.PAYLOAD_TOO_LARGE + 
                    String.format(": %d bytes (max: %d bytes)", payloadSize, Constants.Limits.MAX_PAYLOAD_SIZE_BYTES));
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_PAYLOAD_FORMAT, e);
        }
    }
}

