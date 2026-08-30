pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "eventforge"

include(
    "common-events",
    "common-testing",
    "order-service",
    "payment-service",
    "inventory-service",
    "notification-service",
    "e2e-tests"
)
