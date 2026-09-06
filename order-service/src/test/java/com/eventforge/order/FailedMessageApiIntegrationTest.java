package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStatus;
import com.eventforge.events.failure.FailedMessageStore;
import com.eventforge.order.saga.SagaOrchestrator;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * The operator surface over captured failures, asserted at the HTTP layer against the real filter
 * chain — the split that matters is that <em>seeing</em> a failure and <em>acting</em> on one are
 * different privileges.
 *
 * <p>This is the server-side half of the console's forthcoming "unauthorized replay is denied"
 * evidence. A console that hides the replay button from a VIEWER demonstrates nothing about what
 * the server permits; the rule has to hold against a request that never went near the UI, which is
 * what is asserted here. As in {@code OrderApiAuthorizationIntegrationTest}, no
 * {@code @WithMockUser}: mocking authentication would bypass the chain under test.
 */
@SpringBootTest
@AutoConfigureMockMvc
class FailedMessageApiIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private FailedMessageStore store;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID failedMessageId;

    @BeforeEach
    void captureOneFailure() {
        jdbcTemplate.update("TRUNCATE TABLE failed_messages");
        failedMessageId = UUID.randomUUID();
        store.capture(new FailedMessageRow(
                failedMessageId,
                SagaOrchestrator.CONSUMER_GROUP,
                "payments.events",
                0,
                ThreadLocalRandom.current().nextLong(1_000_000L, Long.MAX_VALUE),
                UUID.randomUUID().toString(),
                "{not-valid-json-at-all",
                null,
                null,
                null,
                "com.fasterxml.jackson.core.JsonParseException: unexpected character",
                FailedMessageStatus.CAPTURED,
                Instant.now(),
                0,
                null,
                null));
    }

    @Test
    void listingFailuresRequiresAuthentication() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/failed-messages"))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());
    }

    /** Seeing what is broken is not a privileged action — a VIEWER is meant to be able to. */
    @Test
    void aViewerCanReadTheFailureList() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/failed-messages")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("viewer", "viewer")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].failedMessageId")
                        .value(failedMessageId.toString()))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].payload").value("{not-valid-json-at-all"));
    }

    /** The one that matters: republishing changes what the system does, so it is OPERATOR-only. */
    @Test
    void aViewerIsForbiddenFromReplaying() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/failed-messages/{id}/replay", failedMessageId)
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("viewer", "viewer")))
                .andExpect(MockMvcResultMatchers.status().isForbidden());

        // Denied at the door, not after the fact: the row must be untouched.
        FailedMessageRow row = store.find(failedMessageId).orElseThrow();
        assertThat(row.status()).isEqualTo(FailedMessageStatus.CAPTURED);
        assertThat(row.replayAttempts()).isZero();
    }

    @Test
    void anOperatorCanReplay() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/failed-messages/{id}/replay", failedMessageId)
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "operator")))
                .andExpect(MockMvcResultMatchers.status().isAccepted());

        FailedMessageRow row = store.find(failedMessageId).orElseThrow();
        assertThat(row.status()).isEqualTo(FailedMessageStatus.REPLAYED);
    }

    /** A second replay is a conflict, not a 404: the row exists, its state just no longer applies. */
    @Test
    void replayingAnAlreadyReplayedMessageConflicts() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/failed-messages/{id}/replay", failedMessageId)
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "operator")))
                .andExpect(MockMvcResultMatchers.status().isAccepted());

        mockMvc.perform(MockMvcRequestBuilders.post("/failed-messages/{id}/replay", failedMessageId)
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "operator")))
                .andExpect(MockMvcResultMatchers.status().isConflict());
    }

    @Test
    void anUnknownIdIsNotFound() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/failed-messages/{id}", UUID.randomUUID())
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("viewer", "viewer")))
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }
}
