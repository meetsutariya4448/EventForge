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
 * <p>{@code OutboxRelayWorker} itself (in {@code com.eventforge.events.outbox}) is the one
 * legitimate reference to {@code KafkaTemplate} in the whole system — this rule allows exactly
 * that package and forbids the dependency everywhere else a service's own classes are scanned.
 */
public final class OutboxOnlyPublishingRule {

    private static final String OUTBOX_PACKAGE = "com.eventforge.events.outbox..";
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
                .resideOutsideOfPackage(OUTBOX_PACKAGE)
                .should()
                .dependOnClassesThat()
                .haveFullyQualifiedName(KAFKA_TEMPLATE)
                .because("every publish must go through OutboxWriter -> OutboxRelayWorker; a direct "
                        + "KafkaTemplate send anywhere else leaves that event unprotected by the outbox "
                        + "transaction and reopens the dual-write bug the pattern exists to close");
        rule.check(classes);
    }
}
