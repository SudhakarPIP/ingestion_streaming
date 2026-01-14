package com.example.ingestion.exception;

import com.example.ingestion.dto.ErrorResponse;
import com.example.ingestion.exception.ValidationException;
import com.example.ingestion.util.Constants;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationExceptions(
            MethodArgumentNotValidException ex, HttpServletRequest request) {
        String errors = ex.getBindingResult().getAllErrors().stream()
                .map(error -> {
                    String fieldName = ((FieldError) error).getField();
                    String errorMessage = error.getDefaultMessage();
                    return fieldName + ": " + errorMessage;
                })
                .collect(Collectors.joining(", "));
        
        ErrorResponse response = new ErrorResponse(
                "Validation error",
                errors,
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(
            ValidationException ex, HttpServletRequest request) {
        log.warn("Validation error: {}", ex.getMessage());
        
        ErrorResponse response = new ErrorResponse(
                "Validation error",
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(
            IllegalArgumentException ex, HttpServletRequest request) {
        log.warn("Illegal argument: {}", ex.getMessage());
        
        ErrorResponse response = new ErrorResponse(
                "Bad request",
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex, HttpServletRequest request) {
        log.warn("Malformed request body: {}", ex.getMessage());
        
        String message = "Malformed request body";
        if (ex.getMessage() != null && ex.getMessage().contains("JSON")) {
            message = "Invalid JSON format";
        }
        
        ErrorResponse response = new ErrorResponse(
                "Bad request",
                message,
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParameter(
            MissingServletRequestParameterException ex, HttpServletRequest request) {
        log.warn("Missing request parameter: {}", ex.getParameterName());
        
        ErrorResponse response = new ErrorResponse(
                "Bad request",
                "Missing required parameter: " + ex.getParameterName(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(
            MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
        log.warn("Type mismatch for parameter: {}", ex.getName());
        
        ErrorResponse response = new ErrorResponse(
                "Bad request",
                "Invalid value for parameter: " + ex.getName(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }
    
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ErrorResponse> handleDataAccessException(
            DataAccessException ex, HttpServletRequest request) {
        log.error("Database error: {}", ex.getMessage(), ex);
        
        ErrorResponse response = new ErrorResponse(
                Constants.ErrorMessages.DATABASE_ERROR,
                "A database error occurred",
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalStateException(
            IllegalStateException ex, HttpServletRequest request) {
        log.error("Illegal state: {}", ex.getMessage(), ex);
        
        ErrorResponse response = new ErrorResponse(
                "Internal server error",
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    @ExceptionHandler(EventNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleEventNotFoundException(
            EventNotFoundException ex, HttpServletRequest request) {
        log.warn("Event not found: {}", ex.getMessage());
        
        ErrorResponse response = new ErrorResponse(
                Constants.ErrorMessages.EVENT_NOT_FOUND,
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }
    
    @ExceptionHandler(BigQueryException.class)
    public ResponseEntity<ErrorResponse> handleBigQueryException(
            BigQueryException ex, HttpServletRequest request) {
        log.error("BigQuery exception: {}", ex.getMessage(), ex);
        
        ErrorResponse response = new ErrorResponse(
                Constants.ErrorMessages.BIGQUERY_INSERT_FAILED,
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ErrorResponse> handleRuntimeException(
            RuntimeException ex, HttpServletRequest request) {
        log.error("Runtime exception: {}", ex.getMessage(), ex);
        
        ErrorResponse response = new ErrorResponse(
                "Internal server error",
                ex.getMessage(),
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleException(
            Exception ex, HttpServletRequest request) {
        log.error("Unexpected exception: {}", ex.getMessage(), ex);
        
        ErrorResponse response = new ErrorResponse(
                "Internal server error",
                Constants.ErrorMessages.UNEXPECTED_ERROR,
                LocalDateTime.now(),
                request.getRequestURI(),
                MDC.get("correlationId")
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}

