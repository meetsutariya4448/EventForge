package com.eventforge.events.outbox;

/** Thrown when the relay fails to publish a claimed row; rolls back the claim (R5: at-least-once). */
public class OutboxRelayException extends RuntimeException {

    public OutboxRelayException(String message, Throwable cause) {
        super(message, cause);
    }
}
