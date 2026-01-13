package com.example.ingestion.exception;

public class EventNotFoundException extends RuntimeException {
    public EventNotFoundException(String message) {
        super(message);
    }
    
    public EventNotFoundException(String tenantId, String eventId) {
        super(String.format("Event not found: tenantId=%s, eventId=%s", tenantId, eventId));
    }
}

