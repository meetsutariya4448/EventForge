# ADR-0001: Build tool — Gradle over Maven

## Context

EventForge is a 6-module Java 21 build: two shared libraries (`common-events`, `common-testing`)
and four Spring Boot services. It needs to be buildable from a genuinely clean clone with a single
documented command, on a machine that may not have a matching JDK pre-installed — the development
machine used to bootstrap this repo only had JDK 22 and 25 installed, no JDK 21.

## Options considered

**Maven**, with a `toolchains.xml`-based JDK selection. Familiar, huge ecosystem, but Maven's
toolchain mechanism requires each developer to have already installed and registered a matching
JDK locally — there is no standard, official "download it if missing" path. On a machine without a
pre-installed JDK 21, a Maven build would simply fail with no build-tool-native remedy.

**Gradle**, with the `org.gradle.toolchains.foojay-resolver-convention` plugin. Gradle's native
Java toolchain support, combined with this plugin, transparently downloads and caches a matching
JDK the first time anyone builds — no developer has to have pre-installed anything beyond a shell
and network access. This was verified directly: the development machine's system JDKs are 22 and
25, and `./gradlew build` provisions a JDK 21 toolchain on first run without any manual step.

## Decision

Gradle (Kotlin DSL), with the Foojay Toolchains Resolver plugin applied in `settings.gradle.kts`
and every module pinned to `JavaLanguageVersion.of(21)` via a `buildSrc` convention plugin
(`eventforge.java-conventions`). This is what makes "one command from a clean clone" actually true
regardless of what JDKs happen to be installed on the machine.

Secondarily: this is a 6-module build wanting consistent conventions (Java version, Spring Boot
starter sets, test framework wiring) across modules. Gradle's `buildSrc` precompiled script
plugins express shared conventions more directly than Maven's parent-POM + BOM inheritance, without
restating dependency versions in every module.

Both Gradle and Maven ship self-provisioning wrappers (`gradlew`/`mvnw`), so wrapper-based
onboarding itself is a wash between the two — the deciding factor is the JDK toolchain story.

## Consequences

- Every module applies `eventforge.java-conventions` or `eventforge.spring-service-conventions`
  from `buildSrc` rather than repeating plugin/version boilerplate.
- Anyone building EventForge needs network access on first build (to fetch the Gradle wrapper
  distribution, if not cached, and the JDK 21 toolchain). This is a one-time cost, cached
  afterward in `~/.gradle`.
- Contributors need to know Gradle Kotlin DSL, not Maven XML, to modify the build.

## Revisit if

The project needs to interoperate with a Maven-only internal tooling chain, or Gradle's toolchain
auto-provisioning becomes unreliable in a target CI environment without documented workarounds.
