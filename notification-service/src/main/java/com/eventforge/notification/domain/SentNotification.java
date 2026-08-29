package com.eventforge.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Stands in for an external side effect (an email/SMS provider call). Deliberately has no unique
 * constraint on {@code order_id} the way {@code payments}/{@code inventory_order_events} do — this
 * table exists specifically to show what happens without a second, local safety net: only
 * {@code processed_events}' dedupe protects it (see the dedupe-key/retention-window ADR).
 */
@Entity
@Table(name = "sent_notifications")
public class SentNotification {

    @Id
    @Column(name = "notification_id")
    private UUID notificationId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "channel", nullable = false)
    private String channel;

    @Column(name = "sent_at", nullable = false)
    private Instant sentAt;

    protected SentNotification() {}

    public SentNotification(UUID notificationId, String orderId, String channel, Instant sentAt) {
        this.notificationId = notificationId;
        this.orderId = orderId;
        this.channel = channel;
        this.sentAt = sentAt;
    }

    public UUID getNotificationId() {
        return notificationId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getChannel() {
        return channel;
    }

    public Instant getSentAt() {
        return sentAt;
    }
}
