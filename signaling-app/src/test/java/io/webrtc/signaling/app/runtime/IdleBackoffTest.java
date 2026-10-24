package io.webrtc.signaling.app.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class IdleBackoffTest {
    private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);

    @Test
    void idleScansDoubleTheirPauseUpToTheCapAndWorkResetsIt() {
        var backoff = new IdleBackoff(100, 1000);
        long now = 0;
        assertThat(backoff.due(now)).isTrue();
        backoff.completed(false, now);
        assertThat(backoff.due(now + 199 * MS)).isFalse();
        assertThat(backoff.due(now + 200 * MS)).isTrue();
        for (int i = 0; i < 10; i++) backoff.completed(false, now);
        assertThat(backoff.due(now + 999 * MS)).isFalse();
        assertThat(backoff.due(now + 1000 * MS)).isTrue();
        backoff.completed(true, now);
        assertThat(backoff.due(now)).isTrue();
        backoff.completed(false, now);
        assertThat(backoff.due(now + 200 * MS)).isTrue();
    }
}
