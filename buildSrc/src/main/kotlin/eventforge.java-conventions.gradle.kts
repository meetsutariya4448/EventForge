plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

// Every Testcontainers-backed test class boots its own real Postgres/Kafka containers. Running
// more than one such test JVM at a time has produced real, intermittent failures under this
// project's constrained local Docker memory allocation (MultiWorkerRelayOrderingIntegrationTest,
// RelayAsyncGapTraceIntegrationTest, OutboxRelayCrashWindowIntegrationTest — all pass every time
// in isolation, only flake under full-suite concurrent load; see README "Known limitations").
//
// maxParallelForks = 1 (already Gradle's default, made explicit here so a future change doesn't
// silently reintroduce concurrent forks within one module's own `test` task) handles WITHIN a
// single module. It does not handle ACROSS modules: `./gradlew test` at the root only serializes
// different subprojects' `test` tasks if project-parallel execution is off, and that is NOT
// something this file can guarantee — `org.gradle.parallel=true` in a developer's own
// `GRADLE_USER_HOME/gradle.properties` overrides a project-committed `gradle.properties` setting
// (verified directly: a project-root `org.gradle.parallel=false` does not win against a
// GRADLE_USER_HOME value of `true`), so this repository cannot force sequential cross-module
// execution through gradle.properties alone. A shared build service with maxParallelUsages = 1,
// applied to every Test task via usesService(...), is enforced by Gradle's task scheduler itself
// regardless of project-parallel settings — this is what actually guarantees no two
// Testcontainers-backed test JVMs (in any module) run at the same time.
abstract class TestcontainersExecutionLock : BuildService<BuildServiceParameters.None>

val testcontainersExecutionLock =
    gradle.sharedServices.registerIfAbsent("testcontainersExecutionLock", TestcontainersExecutionLock::class) {
        maxParallelUsages.set(1)
    }

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    maxParallelForks = 1
    usesService(testcontainersExecutionLock)
}
