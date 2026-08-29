package com.eventforge.notification.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SentNotificationRepository extends JpaRepository<SentNotification, UUID> {
    List<SentNotification> findByOrderId(String orderId);
}
