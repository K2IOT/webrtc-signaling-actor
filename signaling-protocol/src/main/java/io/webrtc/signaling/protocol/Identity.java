package io.webrtc.signaling.protocol;

import com.fasterxml.jackson.annotation.JsonValue;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

public final class Identity {
    private Identity() {
    }

    static String text(String value, int maxBytes) {
        Objects.requireNonNull(value);
        if (value.isBlank()
                || !value.equals(value.strip())
                || value.getBytes(StandardCharsets.UTF_8).length > maxBytes
                || value
                .codePoints()
                .anyMatch(c -> Character.isISOControl(c) || c >= 0xd800 && c <= 0xdfff))
            throw new IllegalArgumentException("invalid bounded identifier");
        return value;
    }

    static void positive(long value) {
        if (value <= 0) throw new IllegalArgumentException("counter must be positive");
    }

    // Canonical user IDs are NFC UTF-8, case-sensitive, no trim/case folding.
    public record UserId(String value) {
        public UserId {
            value = text(Normalizer.normalize(Objects.requireNonNull(value), Normalizer.Form.NFC), 256);
        }

        @JsonValue
        public String value() {
            return value;
        }
    }

    public record SessionKey(String issuer, String jti) {
        public SessionKey {
            issuer = text(issuer, 512);
            jti = text(jti, 256);
        }
    }

    public record SessionIncarnation(UUID value) {
        public SessionIncarnation {
            Objects.requireNonNull(value);
        }
    }

    public record RequestId(UUID value) {
        public RequestId {
            Objects.requireNonNull(value);
        }
    }

    public record CallId(String value) {
        private static final Pattern FORMAT =
                Pattern.compile(
                        "([a-z][a-z0-9-]{0,23})\\.e([1-9][0-9]*)\\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

        public CallId {
            value = text(value, 96);
            var match = FORMAT.matcher(value);
            if (!match.matches()) throw new IllegalArgumentException("invalid routed call ID");
            positive(Long.parseLong(match.group(2)));
            UUID.fromString(match.group(3));
        }

        public static CallId create(String cell, long epoch) {
            positive(epoch);
            return new CallId(cell + ".e" + epoch + "." + UUID.randomUUID());
        }

        /**
         * The call an INVITE creates, derived from its idempotency key (issuer, jti, requestId) so
         * that every retry reaches the same entity and group. While the stored INVITE result lives
         * (24 h after its final outcome) a reused key replays it; once the result is gone, a call row
         * that still exists (a call longer than that) makes the create fail on the primary key, never
         * reuse that call. A UUIDv8 of SHA-256 over length-prefixed fields; routing only, never
         * authorization.
         */
        public static CallId forInvite(String cell, long epoch, SessionKey sender, RequestId request) {
            positive(epoch);
            try {
                var digest = java.security.MessageDigest.getInstance("SHA-256");
                for (String part :
                        new String[]{
                                "invite-call-id-v1", cell, Long.toString(epoch), sender.issuer(), sender.jti(),
                                request.value().toString()
                        }) {
                    byte[] bytes = part.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());
                    digest.update(bytes);
                }
                var hash = java.nio.ByteBuffer.wrap(digest.digest());
                long high = (hash.getLong() & ~0xF000L) | 0x8000L;
                long low = (hash.getLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
                return new CallId(cell + ".e" + epoch + "." + new UUID(high, low));
            } catch (java.security.NoSuchAlgorithmException unavailable) {
                throw new IllegalStateException(unavailable);
            }
        }

        public String coordinatorCell() {
            return value.substring(0, value.indexOf('.'));
        }

        public long routingEpoch() {
            return Long.parseLong(value.substring(value.indexOf(".e") + 2, value.lastIndexOf('.')));
        }

        @JsonValue
        public String value() {
            return value;
        }
    }

    public record CallVersion(long value) {
        public CallVersion {
            positive(value);
        }

        @JsonValue
        public String decimal() {
            return Long.toString(value);
        }
    }

    public record NegotiationId(long value) {
        public NegotiationId {
            positive(value);
        }

        @JsonValue
        public String decimal() {
            return Long.toString(value);
        }
    }

    public record IceGeneration(long value) {
        public IceGeneration {
            positive(value);
        }

        @JsonValue
        public String decimal() {
            return Long.toString(value);
        }
    }

    public record CommandScope(String value) {
        public CommandScope {
            value = text(value, 128);
            if (!value.equals("INVITE")) {
                if (!value.startsWith("CALL:")) throw new IllegalArgumentException("invalid command scope");
                new CallId(value.substring(5));
            }
        }

        public static CommandScope invite() {
            return new CommandScope("INVITE");
        }

        public static CommandScope call(CallId id) {
            return new CommandScope("CALL:" + id.value());
        }
    }

    public record AuthenticatedSession(
            UserId userId,
            SessionKey key,
            SessionIncarnation incarnation,
            long connectionGeneration,
            UUID connectionId) {
        public AuthenticatedSession {
            Objects.requireNonNull(userId);
            Objects.requireNonNull(key);
            Objects.requireNonNull(incarnation);
            Objects.requireNonNull(connectionId);
            positive(connectionGeneration);
        }
    }
}
