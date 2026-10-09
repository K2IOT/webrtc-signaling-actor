package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.storage.worker.*;
import java.time.*;
import java.util.*;
import java.util.function.LongSupplier;

/** Verified scoped facts for edge admission. Readers do bounded cached work, with no SQL or network. */
public final class NativeCachedSecurity {
    private record State(long offset, Instant latestReport, Instant checkedAt, long requestStarted,
                         Map<String, Long> epochs, Set<String> retired) {}
    private final IdentitySecurityContract identity;
    private final RevocationSourceVerifier verifier;
    private final int capacity;
    private final Clock wall;
    private final LongSupplier elapsed;
    private final Duration freshness;
    private volatile State state = new State(0, null, null, 0, Map.of(), Set.of());

    public NativeCachedSecurity(IdentitySecurityContract identity, RevocationSourceVerifier verifier,
                                int capacity, Clock wall, LongSupplier elapsed) {
        this.identity = Objects.requireNonNull(identity);
        this.verifier = Objects.requireNonNull(verifier);
        if (capacity < 1 || capacity > 65536) throw new IllegalArgumentException("Bounded scoped security cache required");
        this.capacity = capacity;
        this.wall = Objects.requireNonNull(wall);
        this.elapsed = Objects.requireNonNull(elapsed);
        freshness = identity.hardSafetyBound().compareTo(Duration.ofSeconds(5)) < 0 ? identity.hardSafetyBound() : Duration.ofSeconds(5);
    }

    /** One worker publishes an immutable page atomically. Invalid input closes admission without forgetting revocations. */
    public synchronized boolean apply(RevocationReconciler.Batch batch, long requestStarted) {
        var previous = state;
        try {
            var now = wall.instant();
            long age = elapsed.getAsLong() - requestStarted;
            if (batch == null || batch.fromOffset() != previous.offset() || !verifier.test(batch)
                    || batch.checkedAt().isAfter(now) || !now.isBefore(batch.checkedAt().plus(freshness))
                    || age < 0 || age >= freshness.toNanos()
                    || previous.latestReport() != null && !batch.checkedAt().isAfter(previous.latestReport()))
                throw new IllegalArgumentException("Invalid scoped source page");
            var epochs = new HashMap<>(previous.epochs());
            var retired = new HashSet<>(previous.retired());
            var offsets = new HashSet<Long>();
            for (var event : batch.events()) {
                if (!identity.issuer().equals(event.issuer()) || event.sourceOffset() <= batch.fromOffset()
                        || !offsets.add(event.sourceOffset())) throw new IllegalArgumentException("Invalid scoped source ordering");
                epochs.merge(RevocationReconciler.subject(event.userId(), event.jti()), event.epoch(), Math::max);
            }
            for (var key : batch.retiredKeys()) {
                if (!identity.issuer().equals(key.issuer()) || !offsets.add(key.sourceOffset()))
                    throw new IllegalArgumentException("Invalid scoped retirement ordering");
                retired.add(key.signingKeyId());
            }
            // Never evict a security fact to admit a newer page. Re-enrollment is required at this bound.
            if (epochs.size() + retired.size() > capacity) throw new IllegalStateException("Scoped cache capacity exhausted");
            state = new State(batch.highWater(), batch.checkedAt(), batch.caughtUp() ? batch.checkedAt() : null,
                    requestStarted, Map.copyOf(epochs), Set.copyOf(retired));
            return true;
        } catch (RuntimeException invalid) {
            invalidate();
            return false;
        }
    }

    public synchronized void invalidate() {
        var previous = state;
        state = new State(previous.offset(), previous.latestReport(), null, 0, previous.epochs(), previous.retired());
    }
    public long offset() { return state.offset(); }
    public int retainedEntries() { var current = state; return current.epochs().size() + current.retired().size(); }
    public boolean usable() { return fresh(state, wall.instant()); }
    private boolean fresh(State current, Instant now) {
        long age = elapsed.getAsLong() - current.requestStarted();
        return current.checkedAt() != null && age >= 0 && age < freshness.toNanos()
                && !current.checkedAt().isAfter(now) && now.isBefore(current.checkedAt().plus(freshness));
    }
    public AuthorizationStatus check(AuthPrincipal principal, Instant now) {
        Objects.requireNonNull(principal); Objects.requireNonNull(now);
        if (!principal.expiresAt().isAfter(now)) return AuthorizationStatus.TOKEN_EXPIRED;
        var current = state;
        if (!fresh(current, now) || !identity.issuer().equals(principal.key().issuer())) return AuthorizationStatus.FRESHNESS_UNKNOWN;
        if (current.retired().contains(principal.signingKeyId())
                || current.epochs().getOrDefault(RevocationReconciler.subject(principal.userId(), null), -1L) >= principal.securityEpoch()
                || current.epochs().getOrDefault(RevocationReconciler.subject(principal.userId(), principal.key().jti()), -1L) >= principal.securityEpoch())
            return AuthorizationStatus.REVOKED;
        // A concurrent source invalidation cannot authorize through the older snapshot.
        return state == current && fresh(current, now) ? AuthorizationStatus.ALLOWED : AuthorizationStatus.FRESHNESS_UNKNOWN;
    }
}
