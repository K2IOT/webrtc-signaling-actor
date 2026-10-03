package io.webrtc.signaling.app.config;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Immutable effective configuration; unsafe values fail binding, before readiness. */
@ConfigurationProperties(prefix = "signaling", ignoreUnknownFields = false)
public record SignalingProperties(Identity identity, Lease lease, Cluster cluster,
        Protocol protocol, Transport transport, Queues queues) {
    public SignalingProperties {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(queues, "queues");
    }

    public String fingerprint() {
        // Length-prefix sections so identity text cannot alias delimiters in the fingerprint.
        StringBuilder canonical = new StringBuilder("signaling-config-v1:");
        for (Object section : new Object[]{identity.issuer(), identity.audience(), identity.clockSkew(), lease, cluster, protocol, transport, queues}) {
            String value = section.toString();
            canonical.append(value.length()).append(':').append(value);
        }
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public record Identity(String issuer, String audience, Duration clockSkew) {
        public Identity {
            identifier(issuer, 512, "issuer");
            identifier(audience, 512, "audience");
            require(clockSkew != null && !clockSkew.isNegative()
                && clockSkew.compareTo(Duration.ofSeconds(30)) <= 0, "clock skew exceeds 30s");
        }
    }
    public record Lease(Duration ttl, Duration renewal) {
        public Lease {
            positive(ttl, "lease TTL"); positive(renewal, "lease renewal");
            require(renewal.compareTo(ttl) < 0, "renewal must precede expiry");
            require(ttl.equals(Duration.ofSeconds(15)) && renewal.equals(Duration.ofSeconds(5)),
                "v1 lease contract requires TTL15s/renewal5s");
        }
    }
    public record Cluster(Duration sbrStableAfter, Duration downRemovalMargin, Duration terminationGrace,
            Duration shutdownPhaseBudget, int actorPods, int actorSurge, Duration userPassivation) {
        public Cluster {
            positive(sbrStableAfter, "SBR stability"); positive(downRemovalMargin, "removal margin");
            positive(terminationGrace, "termination grace"); positive(shutdownPhaseBudget, "shutdown budget");
            positive(userPassivation, "passivation");
            require(sbrStableAfter.compareTo(Duration.ofSeconds(10)) >= 0
                && downRemovalMargin.compareTo(Duration.ofSeconds(10)) >= 0, "SBR margins below baseline");
            require(actorPods == 6 && actorSurge >= 0 && actorSurge <= 1, "unqualified actor membership");
            require(terminationGrace.compareTo(Duration.ofSeconds(90)) >= 0
                && shutdownPhaseBudget.plus(sbrStableAfter).plus(downRemovalMargin).compareTo(terminationGrace) < 0,
                "shutdown budget and margins require termination headroom");
        }
    }
    public record Protocol(int version, int frameBytes, int jwtBytes, int sdpBytes,
            int iceCandidateBytes, int iceBatchCount, int iceBatchBytes, int iceRoundCount,
            int iceRoundBytes, int envelopeBytes) {
        public Protocol {
            require(version == 1, "unsupported protocol version");
            limit(frameBytes, 81920, "frame"); limit(jwtBytes, 8192, "JWT");
            limit(sdpBytes, 65536, "SDP"); limit(iceCandidateBytes, 2048, "ICE candidate");
            limit(iceBatchCount, 20, "ICE batch count"); limit(iceBatchBytes, 8192, "ICE batch bytes");
            limit(iceRoundCount, 256, "ICE round count"); limit(iceRoundBytes, 262144, "ICE round bytes");
            limit(envelopeBytes, 98304, "actor envelope");
            require(jwtBytes <= frameBytes && sdpBytes < frameBytes && iceBatchBytes < frameBytes
                && frameBytes < envelopeBytes && iceCandidateBytes <= iceBatchBytes
                && iceBatchCount <= iceRoundCount && iceBatchBytes <= iceRoundBytes, "inconsistent wire budgets");
        }
    }
    public record Transport(Duration authTimeout, Duration heartbeat, Duration pongDeadline,
            Duration edgeIdleTimeout, Duration controlDeadline, Duration relayDeadline, int maxRetries) {
        public Transport {
            positive(authTimeout, "AUTH timeout"); positive(heartbeat, "heartbeat");
            positive(pongDeadline, "Pong deadline"); positive(edgeIdleTimeout, "edge idle timeout");
            positive(controlDeadline, "control deadline"); positive(relayDeadline, "relay deadline");
            require(authTimeout.compareTo(Duration.ofSeconds(5)) <= 0, "AUTH timeout exceeds 5s");
            require(heartbeat.equals(Duration.ofSeconds(30)) && pongDeadline.equals(Duration.ofSeconds(10)),
                "heartbeat contract is 30s/10s");
            require(edgeIdleTimeout.compareTo(Duration.ofSeconds(90)) >= 0, "edge idle timeout below 90s");
            require(controlDeadline.compareTo(Duration.ofSeconds(2)) <= 0
                && relayDeadline.compareTo(Duration.ofSeconds(1)) <= 0
                && relayDeadline.compareTo(controlDeadline) <= 0, "RPC deadline exceeds contract");
            require(maxRetries >= 0 && maxRetries <= 2, "retry amplification");
        }
    }
    public record Queues(int entityMessages, int entityBytes, int mailboxMessages, int ingressMessages,
            int ingressBytes, int outboundChannelBytes, int outboundAggregateBytes) {
        public Queues {
            limit(entityMessages, 64, "entity messages"); limit(entityBytes, 262144, "entity bytes");
            limit(mailboxMessages, 128, "mailbox"); limit(ingressMessages, 4096, "ingress messages");
            limit(ingressBytes, 33554432, "ingress bytes"); limit(outboundChannelBytes, 262144, "socket outbound");
            limit(outboundAggregateBytes, 268435456, "gateway outbound");
            require(entityMessages < mailboxMessages, "mailbox must reserve completion capacity");
            require(entityBytes <= ingressBytes && outboundChannelBytes <= outboundAggregateBytes,
                "aggregate budgets below entity budgets");
        }
    }
    private static void positive(Duration value, String name) {
        require(value != null && !value.isNegative() && !value.isZero(), name + " must be positive");
    }
    private static void identifier(String value, int maxBytes, String name) {
        require(value != null && !value.isBlank() && !value.contains("${")
            && value.getBytes(StandardCharsets.UTF_8).length <= maxBytes
            && value.codePoints().noneMatch(Character::isISOControl), "invalid " + name);
    }
    private static void limit(int value, int max, String name) {
        require(value > 0 && value <= max, "invalid " + name + " limit");
    }
    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException(reason);
    }
}
