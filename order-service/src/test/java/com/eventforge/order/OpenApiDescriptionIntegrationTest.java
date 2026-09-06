package com.eventforge.order;

import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * The OpenAPI description is a build input, not documentation: the console's TypeScript types are
 * generated from it, so if it stops describing an endpoint, the console silently loses the types
 * for that endpoint rather than failing loudly.
 *
 * <p>This test exists because the alternative is trusting that adding a dependency worked.
 * {@code EventForgeSecurityAutoConfiguration} has permitted {@code /v3/api-docs} since the
 * security step, but nothing served it until now — the permit described an endpoint that did not
 * exist, and no test noticed. That is exactly the failure this guards against repeating.
 *
 * <p>It asserts the two contracts the console actually consumes, not the whole document: creating
 * an order, and the failed-message endpoints. Asserting the full schema would fail on every
 * unrelated change and teach whoever hits it to regenerate the expectation without reading it.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OpenApiDescriptionIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    /** Unauthenticated, deliberately: type generation is a build step, not a logged-in user. */
    @Test
    void theDescriptionIsServedWithoutCredentials() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/v3/api-docs"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.openapi").exists());
    }

    @Test
    void itDescribesTheEndpointsTheConsoleGeneratesTypesFrom() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/v3/api-docs"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.paths['/orders'].post").exists())
                .andExpect(MockMvcResultMatchers.jsonPath("$.paths['/failed-messages'].get").exists())
                .andExpect(MockMvcResultMatchers.jsonPath("$.paths['/failed-messages/{id}'].get").exists())
                .andExpect(MockMvcResultMatchers.jsonPath("$.paths['/failed-messages/{id}/replay'].post")
                        .exists());
    }
}
