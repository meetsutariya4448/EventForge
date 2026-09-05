package com.eventforge.events.security;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The accounts every EventForge service authenticates against.
 *
 * <p>Note what is deliberately absent: an {@code enabled} flag. Security is not switchable here.
 * ADR-0006 established that this project's fault seam exists to inject failures, not to turn
 * invariants off, and a test that runs with authentication disabled proves nothing about the
 * filter chain that actually ships. Tests authenticate instead.
 *
 * @param users account name to credentials. Passwords carry a Spring Security encoding prefix
 *     ({@code {noop}}, {@code {bcrypt}}, …) so the deployed form can be a real hash without the
 *     local form needing to be.
 */
@ConfigurationProperties(prefix = "eventforge.security")
public record SecurityProperties(Map<String, User> users) {

    /**
     * @param password prefixed with its encoding, per Spring Security's delegating encoder.
     * @param roles granted without the {@code ROLE_} prefix — {@code OPERATOR}, not
     *     {@code ROLE_OPERATOR}; Spring adds it.
     */
    public record User(String password, List<String> roles) {}
}
