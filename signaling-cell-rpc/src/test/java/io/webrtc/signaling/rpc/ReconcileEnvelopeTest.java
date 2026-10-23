package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.admission.CompletionReceipt;
import io.webrtc.signaling.actors.admission.EntityAdmission;
import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.cluster.ApplicationSerializer;
import io.webrtc.signaling.actors.cluster.NativeEnvelopeAdmission;
import io.webrtc.signaling.protocol.Identity.CallId;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The recovery reconcile envelope is built several times while it is admitted (once to size it, once
 * to send it). It must not read the clock each time: the serialized length depends on the instant.
 */
class ReconcileEnvelopeTest {
    /** A clock whose every reading has a different number of significant nanosecond digits. */
    private static Clock drifting() {
        var reads = new AtomicInteger();
        return new Clock() {
            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                int n = reads.getAndIncrement();
                return Instant.ofEpochSecond(1_792_760_692L, n % 2 == 0 ? 100_000_000L : 123_456_789L);
            }
        };
    }

    @Test
    void reconcileEnvelopeIsStableWhileTheClockMoves() {
        var kit = ActorTestKit.create();
        try {
            var serializer = new ApplicationSerializer(Adapter.toClassic(kit.system()));
            var admission = new EntityAdmission(8, 8192, 2, 1024, 64, 32768);
            var receipts = kit.<CompletionReceipt.PhysicalDone>createTestProbe();
            var receipt = new CompletionReceipt(UUID.randomUUID(), receipts.ref());
            var call = CallId.create("c001", 1);
            var sent = new AtomicReference<CallActor.ReconcileWake>();
            try (var ticket = admission.acquire("call:" + call.value(), EntityAdmission.Priority.NORMAL, 1)) {
                NativeEnvelopeAdmission.send(
                        ticket,
                        ShardedActorIngress.reconcileFactory(call, receipt, drifting(), Duration.ofMillis(250)),
                        sent::set,
                        serializer);
            }
            assertThat(sent.get()).isNotNull();
        } finally {
            kit.shutdownTestKit();
        }
    }
}
