package io.webrtc.signaling.auth;

import io.webrtc.signaling.protocol.Identity.UserId;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

final class TestSecurityStore implements RevocationState.Store {
    final Map<Key, Long> epochs = new HashMap<>();
    RevocationState.Progress progress;

    public synchronized boolean apply(RevocationState.Event e) {
        var k = new Key(e.issuer(), e.userId(), e.jti());
        if (epochs.getOrDefault(k, -1L) >= e.epoch()) return false;
        epochs.put(k, e.epoch());
        return true;
    }

    public synchronized long epoch(String issuer, UserId user, String jti) {
        return epochs.getOrDefault(new Key(issuer, user, jti), -1L);
    }

    public synchronized RevocationState.Progress progress() {
        return progress;
    }

    public synchronized void reconcile(long offset, Instant at) {
        if (progress == null || offset >= progress.offset())
            progress = new RevocationState.Progress(offset, at);
    }

    record Key(String issuer, UserId user, String jti) {
    }
}
