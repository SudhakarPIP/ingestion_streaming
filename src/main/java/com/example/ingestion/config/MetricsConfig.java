package com.example.ingestion.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter eventsIngestedTotal(MeterRegistry registry) {
        return Counter.builder("events_ingested_total")
                .description("Total number of events ingested")
                .register(registry);
    }

    @Bean
    public Counter outboxClaimedTotal(MeterRegistry registry) {
        return Counter.builder("outbox_claimed_total")
                .description("Total number of outbox records claimed for processing")
                .register(registry);
    }

    @Bean
    public Counter outboxDoneTotal(MeterRegistry registry) {
        return Counter.builder("outbox_done_total")
                .description("Total number of outbox records successfully processed")
                .register(registry);
    }

    @Bean
    public Counter outboxFailedTotal(MeterRegistry registry) {
        return Counter.builder("outbox_failed_total")
                .description("Total number of outbox records that failed processing")
                .register(registry);
    }

    @Bean
    public Counter bigqueryInsertSuccessTotal(MeterRegistry registry) {
        return Counter.builder("bigquery_insert_success_total")
                .description("Total number of successful BigQuery insertions")
                .register(registry);
    }

    @Bean
    public Counter bigqueryInsertFailureTotal(MeterRegistry registry) {
        return Counter.builder("bigquery_insert_failure_total")
                .description("Total number of failed BigQuery insertions")
                .register(registry);
    }
}

