package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * The sequential half of {@code Idempotency-Key} on {@code POST /orders}: a retry replays rather
 * than creating a second order, a reused key with a changed body is refused, and a request
 * without the header behaves exactly as it did before the feature existed.
 *
 * <p>The concurrent half — two requests racing the same key — is
 * {@code ConcurrentIdempotentOrderCreationIntegrationTest}, which needs real HTTP because
 * servlet-thread blocking is part of what it proves.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderIdempotencyIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void retryingWithTheSameKeyReplaysTheOriginalResponseAndCreatesNoSecondOrder() throws Exception {
        String key = "retry-" + UUID.randomUUID();
        String body = mapper.writeValueAsString(new CreateOrderRequest(2500, "SKU-RETRY", 2L));

        MvcResult first = post(key, body)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.header().string("Idempotency-Replayed", "false"))
                .andReturn();

        MvcResult second = post(key, body)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.header().string("Idempotency-Replayed", "true"))
                .andReturn();

        String firstBody = first.getResponse().getContentAsString();
        String secondBody = second.getResponse().getContentAsString();
        assertThat(secondBody)
                .as("a replay must return the original response verbatim, not a re-rendered one")
                .isEqualTo(firstBody);

        UUID orderId = UUID.fromString(mapper.readTree(firstBody).get("orderId").asText());
        assertThat(countOrders(orderId)).isEqualTo(1);
        assertThat(countSagas(orderId)).isEqualTo(1);
        assertThat(countOrderCreatedEvents(orderId)).isEqualTo(1);
    }

    @Test
    void reusingAKeyWithADifferentBodyIsRefusedAndLeavesTheOriginalUntouched() throws Exception {
        String key = "mismatch-" + UUID.randomUUID();
        String original = mapper.writeValueAsString(new CreateOrderRequest(2500, "SKU-A", 1L));
        String different = mapper.writeValueAsString(new CreateOrderRequest(9900, "SKU-B", 3L));

        MvcResult first =
                post(key, original).andExpect(MockMvcResultMatchers.status().isCreated()).andReturn();
        UUID orderId = UUID.fromString(
                mapper.readTree(first.getResponse().getContentAsString()).get("orderId").asText());

        // 422, not 500: the mismatch must reach a mapped handler. An unmapped RuntimeException
        // would surface as a server error and misreport a caller bug as our fault.
        post(key, different).andExpect(MockMvcResultMatchers.status().isUnprocessableContent());

        // The refusal rolled back, and rolling back must not have disturbed the committed claim
        // it collided with.
        assertThat(countOrders(orderId)).isEqualTo(1);
        assertThat(totalOrders()).isEqualTo(1);
        String storedFingerprint = jdbcTemplate.queryForObject(
                "SELECT request_fingerprint FROM idempotency_keys WHERE idempotency_key = ?", String.class, key);
        assertThat(storedFingerprint).isNotNull();

        // And the key still works for the request it was actually claimed with.
        post(key, original)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.header().string("Idempotency-Replayed", "true"));
        assertThat(totalOrders()).isEqualTo(1);
    }

    @Test
    void withoutTheHeaderEachRequestCreatesItsOwnOrderAsBefore() throws Exception {
        String body = mapper.writeValueAsString(new CreateOrderRequest(1200, "SKU-NOHEADER", 1L));

        MvcResult first = mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andReturn();
        MvcResult second = mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andReturn();

        UUID firstId = UUID.fromString(
                mapper.readTree(first.getResponse().getContentAsString()).get("orderId").asText());
        UUID secondId = UUID.fromString(
                mapper.readTree(second.getResponse().getContentAsString()).get("orderId").asText());
        assertThat(firstId).isNotEqualTo(secondId);
        assertThat(countOrders(firstId)).isEqualTo(1);
        assertThat(countOrders(secondId)).isEqualTo(1);
    }

    private org.springframework.test.web.servlet.ResultActions post(String idempotencyKey, String body)
            throws Exception {
        return mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", idempotencyKey)
                .content(body));
    }

    private Integer countOrders(UUID orderId) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM orders WHERE order_id = ?", Integer.class, orderId);
    }

    private Integer totalOrders() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM orders", Integer.class);
    }

    private Integer countSagas(UUID orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM saga_instance WHERE order_id = ?", Integer.class, orderId);
    }

    private Integer countOrderCreatedEvents(UUID orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                Integer.class,
                orderId.toString());
    }
}
