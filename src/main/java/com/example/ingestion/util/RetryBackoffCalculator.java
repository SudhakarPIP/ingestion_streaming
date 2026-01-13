package com.example.ingestion.util;

import java.time.LocalDateTime;

public class RetryBackoffCalculator {

    public static LocalDateTime calculateNextRetryAt(int retryCount) {
        int minutes;
        switch (retryCount) {
            case 0:
                minutes = 1;
                break;
            case 1:
                minutes = 5;
                break;
            case 2:
                minutes = 15;
                break;
            case 3:
                minutes = 60;
                break;
            default:
                // After retry 3, keep at 60 minutes
                minutes = 60;
                break;
        }
        return LocalDateTime.now().plusMinutes(minutes);
    }
}

