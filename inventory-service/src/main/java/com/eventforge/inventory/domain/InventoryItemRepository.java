package com.eventforge.inventory.domain;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

public interface InventoryItemRepository extends JpaRepository<InventoryItem, String> {

    /**
     * Takes a real {@code SELECT ... FOR UPDATE} row lock (via JPA's pessimistic write lock mode,
     * ordinary ORM usage — R4's hand-written-SQL carve-out is for the outbox/dedupe/relay-claim
     * mechanisms specifically, not every lock in the system) so two concurrent ReserveInventory
     * commands against the same SKU genuinely serialize rather than racing on a read-then-write.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select i from InventoryItem i where i.sku = :sku")
    Optional<InventoryItem> findWithLockBySku(String sku);
}
