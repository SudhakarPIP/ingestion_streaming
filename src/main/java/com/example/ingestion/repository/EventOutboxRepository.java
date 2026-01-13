package com.example.ingestion.repository;

import com.example.ingestion.entity.EventOutbox;
import com.example.ingestion.entity.EventOutbox.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface EventOutboxRepository extends JpaRepository<EventOutbox, Long> {
    Optional<EventOutbox> findByTenantIdAndEventId(String tenantId, String eventId);
    Optional<EventOutbox> findByEventId(String eventId);
    boolean existsByEventId(String eventId);

    @Modifying
    @Query(value = "UPDATE event_outbox " +
           "SET status = 'PROCESSING', locked_by = :instanceId, locked_at = NOW(3) " +
           "WHERE status = 'PENDING' " +
           "AND next_retry_at <= NOW(3) " +
           "AND (locked_at IS NULL OR locked_at < (NOW(3) - INTERVAL :lockTtlSeconds SECOND)) " +
           "ORDER BY id LIMIT :batchSize", nativeQuery = true)
    int claimPendingRows(@Param("instanceId") String instanceId,
                        @Param("lockTtlSeconds") int lockTtlSeconds,
                        @Param("batchSize") int batchSize);

    @Query(value = "SELECT id, tenant_id, event_id " +
           "FROM event_outbox " +
           "WHERE status = 'PROCESSING' " +
           "AND locked_by = :instanceId " +
           "AND locked_at >= (NOW(3) - INTERVAL :lockTtlSeconds SECOND) " +
           "ORDER BY id", nativeQuery = true)
    List<Object[]> findClaimedRows(@Param("instanceId") String instanceId,
                                   @Param("lockTtlSeconds") int lockTtlSeconds);

    @Query("SELECT COUNT(o) FROM EventOutbox o WHERE o.tenantId = :tenantId " +
           "AND o.createdAt BETWEEN :from AND :to AND o.status = :status")
    Long countByTenantIdAndCreatedAtBetweenAndStatus(@Param("tenantId") String tenantId,
                                                       @Param("from") LocalDateTime from,
                                                       @Param("to") LocalDateTime to,
                                                       @Param("status") OutboxStatus status);

    @Query("SELECT o FROM EventOutbox o WHERE o.status = :status AND o.tenantId = :tenantId " +
           "AND o.retryCount < 5 ORDER BY o.createdAt ASC")
    List<EventOutbox> findFailedByTenantIdForRetry(@Param("tenantId") String tenantId,
                                                     @Param("status") OutboxStatus status,
                                                     org.springframework.data.domain.Pageable pageable);
}

