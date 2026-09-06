package com.eventforge.testing.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;

/**
 * M3 item 7: every service publishing events must go through {@code OutboxWriter} into its own
 * {@code outbox_events} table, relayed by {@code OutboxRelayWorker} — never a direct
 * {@code KafkaTemplate} send from business code. A direct send bypasses the outbox transaction
 * entirely, and the dual-write bug the outbox pattern exists to solve comes back through whichever
 * call site skipped it; only events written through the outbox are protected.
 *
 * <p>Two packages are allowed to touch {@code KafkaTemplate}, and nothing else a service's scan
 * reaches may. {@code com.eventforge.events.outbox} holds {@code OutboxRelayWorker}, the relay the
 * rule exists to funnel every publish through. {@code com.eventforge.events.failure} holds
 * {@code FailedMessageReplayer}, added in v2 WS3 and justified in ADR-0022: a replay is not a dual
 * write, because the durable record it republishes was committed before the publish — the outbox's
 * actual invariant, satisfied by a different table. Widening the allowlist rather than suppressing
 * the rule keeps that a deliberate, reviewable decision instead of an exception someone adds
 * quietly at a third call site.
 */
public final class OutboxOnlyPublishingRule {

    private static final String OUTBOX_PACKAGE = "com.eventforge.events.outbox..";
    private static final String FAILURE_REPLAY_PACKAGE = "com.eventforge.events.failure..";
    private static final String KAFKA_TEMPLATE = "org.springframework.kafka.core.KafkaTemplate";

    private OutboxOnlyPublishingRule() {}

    /**
     * @param basePackage the service's own base package (e.g. {@code "com.eventforge.payment"}) —
     *     scanned alongside {@code com.eventforge.events} so the rule can see both the service's
     *     code and the one legitimate KafkaTemplate reference it's checked against.
     */
    public static void assertOnlyOutboxWriterPublishesToKafka(String basePackage) {
        // DO_NOT_INCLUDE_TESTS: this must check production code only. Test fixtures that
        // legitimately talk to Kafka directly for infrastructure validation rather than business
        // publishing (e.g. the M0 envelope round-trip test) are not the concern this rule guards.
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(basePackage, "com.eventforge.events");
        ArchRule rule = noClasses()
                .that()
                .resideOutsideOfPackages(OUTBOX_PACKAGE, FAILURE_REPLAY_PACKAGE)
                .should()
                .dependOnClassesThat()
                .haveFullyQualifiedName(KAFKA_TEMPLATE)
                .because("every publish must go through OutboxWriter -> OutboxRelayWorker, or else "
                        + "republish something already committed to failed_messages (ADR-0022); a direct "
                        + "KafkaTemplate send anywhere else leaves that event unprotected by the outbox "
                        + "transaction and reopens the dual-write bug the pattern exists to close");
        rule.check(classes);
    }
}
