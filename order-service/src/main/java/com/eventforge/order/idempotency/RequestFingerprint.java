package com.eventforge.order.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The sha256 of a create-order request's meaningful content, used to tell a genuine retry (same
 * key, same body — replay the stored response) from a client bug (same key, different body —
 * refuse, because serving the stored response would describe an order the caller never asked
 * for).
 *
 * <p>Computed over the values {@code CreateOrderRequest}'s accessors return, which is
 * deliberately <em>after</em> its compact constructor has applied defaults. So an empty body and
 * an explicit {@code {"sku":"DEFAULT-SKU","quantity":1}} fingerprint identically — they request
 * the same order, and a retry that spells the defaults out is still a retry.
 */
public final class RequestFingerprint {

    /**
     * ASCII unit separator (U+001F), built numerically rather than written as a literal control
     * character in source — a raw 0x1F byte in a Java file is invisible in most editors and is
     * easily stripped by a diff, a merge, or a copy-paste, which would silently reintroduce the
     * collision this separator exists to prevent. A SKU cannot contain it, so it cannot be
     * forged to manufacture a collision either.
     */
    private static final String SEP = String.valueOf((char) 0x1F);

    private RequestFingerprint() {}

    /**
     * @return 64 lowercase hex characters, matching the {@code CHAR(64)} column it is stored in.
     */
    public static String of(long amountCents, String sku, long quantity) {
        // Delimited rather than plainly concatenated: without a separator, differing splits of
        // the same character sequence across fields would hash identically, so two genuinely
        // different requests could share a fingerprint and one be served the other's response.
        String canonical = amountCents + SEP + sku + SEP + quantity;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java platform; its absence is not a runtime condition
            // this code can meaningfully handle.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
