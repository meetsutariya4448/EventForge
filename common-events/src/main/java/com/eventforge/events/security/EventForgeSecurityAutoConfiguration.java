package com.eventforge.events.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Authentication and role-based authorization for every EventForge service (v2 WS2's access
 * control, landed ahead of the console so the baseline stays green while it is built).
 *
 * <p>Two roles, and the split is the point: {@code VIEWER} can read what the system is doing;
 * {@code OPERATOR} can change it. Every mutation carries
 * {@code @PreAuthorize("hasRole('OPERATOR')")}, so "an unauthorized operator cannot replay a
 * failed message" is enforced by the filter chain and method security rather than by the UI
 * declining to show a button.
 *
 * <p>Stateless HTTP Basic rather than sessions or an external identity provider: the console is
 * the only client, there is no browser session to protect, and standing up an IdP would add a
 * stateful container to a project whose whole local story is one {@code make up}. CSRF is
 * disabled because there is no cookie-borne session for an attacker to ride — the credential
 * travels on every request.
 */
@AutoConfiguration
@ConditionalOnClass(SecurityFilterChain.class)
@EnableConfigurationProperties(SecurityProperties.class)
@EnableMethodSecurity
public class EventForgeSecurityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(SecurityFilterChain.class)
    public SecurityFilterChain eventForgeSecurityFilterChain(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        // Liveness must not require a credential: scripts/start-services.sh polls
                        // it to decide whether the service came up, and the OpenAPI document is
                        // what the console generates its types from.
                        .requestMatchers("/actuator/health", "/actuator/health/**")
                        .permitAll()
                        .requestMatchers("/v3/api-docs", "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html")
                        .permitAll()
                        .anyRequest()
                        .authenticated())
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    /**
     * Spring Security's delegating encoder, so a configured password declares its own encoding.
     * Local config can use {@code {noop}} while a real deployment supplies {@code {bcrypt}}
     * hashes through the environment, with no code change between them.
     */
    @Bean
    @ConditionalOnMissingBean(PasswordEncoder.class)
    public PasswordEncoder eventForgePasswordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    @Bean
    @ConditionalOnMissingBean(UserDetailsService.class)
    public UserDetailsService eventForgeUserDetailsService(SecurityProperties properties) {
        Map<String, SecurityProperties.User> configured = properties.users();
        if (configured == null || configured.isEmpty()) {
            // Failing here beats starting an unusable service: with no accounts, every
            // authenticated route is permanently unreachable and the cause would surface as a
            // uniform 401 with nothing explaining it.
            throw new IllegalStateException(
                    "No accounts configured under eventforge.security.users — every authenticated "
                            + "endpoint would be unreachable. Configure at least one user.");
        }

        List<UserDetails> users = new ArrayList<>();
        configured.forEach((username, user) -> users.add(User.withUsername(username)
                .password(user.password())
                .roles(user.roles().toArray(new String[0]))
                .build()));
        return new InMemoryUserDetailsManager(users);
    }
}
