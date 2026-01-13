package com.example.ingestion.repository;

import com.example.ingestion.entity.EventInbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EventInboxRepository extends JpaRepository<EventInbox, Long> {
    Optional<EventInbox> findByTenantIdAndEventId(String tenantId, String eventId);
    boolean existsByTenantIdAndEventId(String tenantId, String eventId);
}

