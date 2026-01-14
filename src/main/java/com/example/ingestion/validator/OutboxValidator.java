package com.example.ingestion.validator;

import com.example.ingestion.exception.ValidationException;
import com.example.ingestion.util.Constants;
import org.springframework.stereotype.Component;

@Component
public class OutboxValidator {

    public void validateRetryLimit(int limit) {
        if (limit < Constants.Limits.MIN_RETRY_LIMIT || limit > Constants.Limits.MAX_RETRY_LIMIT) {
            throw new ValidationException(Constants.ErrorMessages.INVALID_LIMIT_VALUE);
        }
    }

    public void validateTenantId(String tenantId) {
        if (tenantId != null && !tenantId.trim().isEmpty()) {
            if (tenantId.length() > Constants.Validation.MAX_TENANT_ID_LENGTH) {
                throw new ValidationException(Constants.ErrorMessages.INVALID_TENANT_ID);
            }
        }
    }
}

