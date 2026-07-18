package com.chirag.orderpayment.order.security;

import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Development-only helper: mints a short-lived bearer token so the secured APIs
 * can be exercised with curl/Postman without standing up a full identity provider.
 * Guarded by the {@code dev-token} profile-independent flag; disable in production.
 */
@RestController
public class DevTokenController {

    private final JwtEncoder jwtEncoder;

    public DevTokenController(JwtEncoder jwtEncoder) {
        this.jwtEncoder = jwtEncoder;
    }

    @PostMapping("/api/dev/token")
    public Map<String, String> mint(@RequestParam(defaultValue = "demo-user") String subject) {
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("order-service-dev")
                .subject(subject)
                .issuedAt(now)
                .expiresAt(now.plus(Duration.ofHours(1)))
                .claim("scope", "orders")
                .build();
        String token = jwtEncoder.encode(
                JwtEncoderParameters.from(JwsHeader.with(() -> "HS256").build(), claims))
                .getTokenValue();
        return Map.of("access_token", token, "token_type", "Bearer");
    }
}
