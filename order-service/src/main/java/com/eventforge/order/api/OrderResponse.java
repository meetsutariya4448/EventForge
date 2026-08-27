package com.eventforge.order.api;

import java.util.UUID;

public record OrderResponse(UUID orderId, String status, long amountCents) {}
