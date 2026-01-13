package com.example.ingestion.util;

public final class Constants {
    
    private Constants() {
        throw new UnsupportedOperationException("Utility class");
    }
    
    public static final class Validation {
        private Validation() {}
        
        public static final int MAX_TENANT_ID_LENGTH = 64;
        public static final int MAX_EVENT_ID_LENGTH = 128;
        public static final int MAX_EVENT_TYPE_LENGTH = 64;
        public static final int MAX_SOURCE_LENGTH = 64;
    }
    
    public static final class ErrorMessages {
        private ErrorMessages() {}
        
        public static final String EVENT_NOT_FOUND = "Event not found";
        public static final String INVALID_PAYLOAD_FORMAT = "Invalid payload format";
        public static final String BIGQUERY_INSERT_FAILED = "Failed to insert to BigQuery";
        public static final String UNEXPECTED_ERROR = "An unexpected error occurred";
        public static final String INVALID_TENANT_ID = "Invalid tenant ID format or length";
        public static final String INVALID_EVENT_ID = "Invalid event ID format or length";
        public static final String INVALID_EVENT_TYPE = "Invalid event type format or length";
        public static final String INVALID_EVENT_TIMESTAMP = "Invalid event timestamp";
        public static final String PAYLOAD_TOO_LARGE = "Payload size exceeds maximum allowed";
        public static final String MISSING_REQUIRED_FIELD = "Missing required field";
        public static final String INVALID_LIMIT_VALUE = "Limit must be between 1 and 1000";
        public static final String DATABASE_ERROR = "Database operation failed";
    }
    
    public static final class Limits {
        private Limits() {}
        
        public static final int MAX_PAYLOAD_SIZE_BYTES = 1_000_000; // 1MB
        public static final int MAX_BULK_EVENTS = 1000;
        public static final int MIN_RETRY_LIMIT = 1;
        public static final int MAX_RETRY_LIMIT = 1000;
        public static final int MIN_TENANT_ID_LENGTH = 1;
        public static final int MIN_EVENT_ID_LENGTH = 1;
    }
    
    public static final class Headers {
        private Headers() {}
        
        public static final String CORRELATION_ID = "X-Correlation-Id";
    }
}

