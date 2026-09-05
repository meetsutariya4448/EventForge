package com.eventforge.order;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * Access control asserted from the outside, against the real filter chain: unauthenticated is
 * rejected, an authenticated VIEWER is refused a mutation, and only an OPERATOR gets through.
 *
 * <p>Written now rather than alongside the console, because the console's own "unauthorized
 * replay is denied" evidence is worth very little if the underlying rule was never tested at the
 * HTTP layer — a UI that hides a button proves nothing about what the server permits. This is
 * the server-side half of that claim, on the only mutation that exists today; WS3's replay
 * endpoint inherits the same rule.
 *
 * <p>Note what is <em>not</em> used: {@code @WithMockUser}. Mocked authentication would skip the
 * filter chain that actually ships, which is precisely the thing under test.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderApiAuthorizationIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void anUnauthenticatedRequestIsRejected() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body()))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());
    }

    @Test
    void badCredentialsAreRejected() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "not-the-password"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body()))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());
    }

    /** The one that matters: authenticated, but not authorized to mutate. */
    @Test
    void anAuthenticatedViewerIsForbiddenFromMutating() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("viewer", "viewer"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body()))
                .andExpect(MockMvcResultMatchers.status().isForbidden());
    }

    @Test
    void anOperatorIsAllowedToMutate() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "operator"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body()))
                .andExpect(MockMvcResultMatchers.status().isCreated());
    }

    /** Liveness stays open, or scripts/start-services.sh could never confirm a service came up. */
    @Test
    void healthIsReachableWithoutCredentials() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/actuator/health"))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    private String body() throws Exception {
        return mapper.writeValueAsString(new CreateOrderRequest(1500, "SKU-AUTH", 1L));
    }
}
