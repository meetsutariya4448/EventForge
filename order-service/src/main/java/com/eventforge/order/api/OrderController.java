package com.eventforge.order.api;

import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderService;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final FaultInjector faultInjector;

    public OrderController(OrderService orderService, FaultInjector faultInjector) {
        this.orderService = orderService;
        this.faultInjector = faultInjector;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
            @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "traceparent", required = false) String traceparent,
            @RequestHeader(value = "tracestate", required = false) String tracestate) {
        if (request.amountCents() <= 0) {
            throw new IllegalArgumentException("amountCents must be positive");
        }

        Order order = orderService.createOrder(request.amountCents(), request.sku(), request.quantity(), traceparent, tracestate);

        // The order+outbox transaction has now committed. This is the real seam trap T-worthy
        // fault-injection point exists for: a crash right here would leave a durable, unpublished
        // outbox row for the relay to find and publish later — proving the outbox pattern's point.
        faultInjector.inject(FaultInjectionPoint.AFTER_DB_COMMIT_BEFORE_KAFKA_PUBLISH);

        OrderResponse response = new OrderResponse(order.getOrderId(), order.getStatus(), order.getAmountCents());
        return ResponseEntity.created(URI.create("/orders/" + order.getOrderId())).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
