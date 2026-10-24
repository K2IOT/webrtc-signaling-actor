package io.webrtc.signaling.gateway;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cached-safety watchdog fails closed when the sweep falls behind, never while it keeps time.
 * With at most 2,048 connection slots the sweep has a single slice, so its period equals the
 * interval: the window must still cover the wait for the next planned tick.
 */
class GatewaySecuritySweepFreshnessTest {
    private static GatewayServices services() {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if (method.getName().equals("currentBoot")) return true;
                    if (method.isDefault()) return InvocationHandler.invokeDefault(proxy, method, args);
                    return null;
                };
        return (GatewayServices)
                Proxy.newProxyInstance(
                        GatewayServices.class.getClassLoader(), new Class<?>[]{GatewayServices.class}, handler);
    }

    private static int staleSamples(ConnectionRegistry registry, Duration during) {
        long end = System.nanoTime() + during.toNanos();
        int stale = 0;
        while (System.nanoTime() < end) {
            if (!registry.safetyFresh()) stale++;
            Thread.onSpinWait();
        }
        return stale;
    }

    @Test
    void aSingleSliceSweepThatKeepsTimeIsNeverStale() {
        var registry = new ConnectionRegistry("gw-test", UUID.randomUUID(), 64, 1024);
        try (var sweep =
                     new GatewaySecuritySweep(
                             registry, services(), Clock.systemUTC(), new GatewaySecuritySweep.Settings(Duration.ofMillis(250)))) {
            assertThat(staleSamples(registry, Duration.ofSeconds(2))).isZero();
        }
    }

    @Test
    void aStoppedSweepGoesStaleWithinTwoIntervals() throws Exception {
        var registry = new ConnectionRegistry("gw-test", UUID.randomUUID(), 64, 1024);
        var sweep =
                new GatewaySecuritySweep(
                        registry, services(), Clock.systemUTC(), new GatewaySecuritySweep.Settings(Duration.ofMillis(100)));
        assertThat(registry.safetyFresh()).isTrue();
        sweep.close();

        TimeUnit.MILLISECONDS.sleep(250);

        assertThat(registry.safetyFresh()).isFalse();
    }
}
