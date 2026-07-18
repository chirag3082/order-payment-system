package com.chirag.orderpayment.order.repo;

import com.chirag.orderpayment.order.domain.OutboxEvent;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Fetch the oldest unpublished rows, skipping any locked by another poller
     * instance (SKIP LOCKED). This lets the poller scale horizontally without
     * two instances publishing the same row.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select e from OutboxEvent e where e.published = false order by e.createdAt asc")
    List<OutboxEvent> findUnpublishedForUpdate(Pageable pageable);
}
