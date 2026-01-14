package com.example.ingestion.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "event_inbox", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"tenant_id", "event_id"}, name = "uk_tenant_event")
}, indexes = {
    @Index(columnList = "tenant_id,event_ts", name = "idx_event_ts")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventInbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "event_id", nullable = false, length = 128)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "event_ts", nullable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime eventTs;

    @Column(name = "payload_json", nullable = false, columnDefinition = "JSON")
    private String payloadJson;

    @Column(name = "source", length = 64)
    private String source;

    @Column(name = "checksum", columnDefinition = "CHAR(64)")
    private String checksum;

    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "DATETIME(3)")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}

