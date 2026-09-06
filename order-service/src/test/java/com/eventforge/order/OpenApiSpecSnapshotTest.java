package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

/**
 * Keeps the committed OpenAPI spec identical to what the service actually serves.
 *
 * <h2>Why the spec is committed at all</h2>
 *
 * The console's TypeScript types are generated from it. Generating from a live server would mean
 * type generation needs the whole stack running — Postgres, Kafka, the service — which makes it a
 * thing people skip, and skipped regeneration is exactly how a "generated" client silently becomes
 * a stale hand-written one. A committed spec makes generation a pure file transform.
 *
 * <p>That trade is only safe if the file cannot drift from the server, which is this test's whole
 * job: change a controller without regenerating, and this fails. Without it, committing the spec
 * would have replaced one silent-drift problem with another.
 *
 * <p>Regenerate with {@code ./gradlew :order-service:test --tests '*OpenApiSpecSnapshotTest*'
 * -DupdateOpenApiSpec=true}, then commit the diff — reviewing that diff is the point, since it is
 * the API contract changing.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OpenApiSpecSnapshotTest extends AbstractPostgresKafkaIntegrationTest {

    /** Lives next to the console that consumes it, not in docs/ — it is a build input, not prose. */
    private static final Path SPEC = Path.of("..", "console", "openapi", "order-service.json");

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper mapper =
            new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, SerializationFeature.INDENT_OUTPUT);

    @Test
    void theCommittedSpecMatchesWhatTheServiceServes() throws Exception {
        String served = mockMvc.perform(MockMvcRequestBuilders.get("/v3/api-docs"))
                .andReturn()
                .getResponse()
                .getContentAsString();

        // Normalized before comparing: key order is not part of the contract, and comparing raw
        // bytes would produce spurious failures on an irrelevant reordering.
        String normalized = mapper.writeValueAsString(mapper.readValue(served, JsonNode.class));

        if (Boolean.getBoolean("updateOpenApiSpec")) {
            Files.createDirectories(SPEC.getParent());
            Files.writeString(SPEC, normalized + System.lineSeparator());
            return;
        }

        assertThat(Files.exists(SPEC))
                .as("%s is missing. Regenerate it with -DupdateOpenApiSpec=true and commit it.", SPEC)
                .isTrue();

        String committed = Files.readString(SPEC).strip();
        assertThat(committed)
                .as("The committed OpenAPI spec no longer matches what the service serves, so the "
                        + "console's generated types describe an API that does not exist. Regenerate "
                        + "with -DupdateOpenApiSpec=true and review the diff — it is the API contract "
                        + "changing.")
                .isEqualTo(normalized.strip());
    }
}
