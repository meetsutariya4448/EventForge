package com.eventforge.inventory.domain;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryOrderEventRepository extends JpaRepository<InventoryOrderEvent, UUID> {}
