package com.example.ingestion.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "event_outbox", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"tenant_id", "event_id"}, name = "uk_outbox_item")
}, indexes = {
    @Index(columnList = "status,next_retry_at,locked_at", name = "idx_pick"),
    @Index(columnList = "tenant_id,created_at", name = "idx_tenant_created")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "event_id", nullable = false, length = 128)
    private String eventId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, columnDefinition = "VARCHAR(16)")
    private OutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "next_retry_at", nullable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime nextRetryAt;

    @Column(name = "last_error", length = 2048)
    private String lastError;

    @Column(name = "locked_by", length = 64)
    private String lockedBy;

    @Column(name = "locked_at", columnDefinition = "DATETIME(3)")
    private LocalDateTime lockedAt;

    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
        if (nextRetryAt == null) {
            nextRetryAt = now;
        }
        if (status == null) {
            status = OutboxStatus.PENDING;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public enum OutboxStatus {
        PENDING,
        PROCESSING,
        DONE,
        FAILED
    }
}

