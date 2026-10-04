package io.webrtc.signaling.actors.cluster;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.admission.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.junit.jupiter.api.Test;

class NativeEnvelopeAdmissionTest {
    @Test void chargesActualSerializedEnvelopeBeforeTellAndKnownPreDispatchRejectionRetiresCollector() throws Exception {
        var kit = ActorTestKit.create();
        try {
            var serializer = new ApplicationSerializer(Adapter.toClassic(kit.system()));
            var admission = new EntityAdmission(8, 8192, 2, 1024, 64, 32768);
            var probe = kit.<UserCommand.Result>createTestProbe(); var receipts = kit.<CompletionReceipt.PhysicalDone>createTestProbe();
            var request = request(); var deadline = Instant.now().plusSeconds(1); var receipt = new CompletionReceipt(UUID.randomUUID(), receipts.ref());
            var sent = new AtomicReference<UserCommand.Mutate>();
            try (var ticket = admission.acquire("user:alice", EntityAdmission.Priority.NORMAL, 1)) {
                NativeEnvelopeAdmission.send(ticket, bytes -> new UserCommand.Mutate(new UserCommand.Reserve(request), probe.ref(), deadline, bytes, receipt), sent::set, serializer);
                assertThat(sent.get()).isNotNull();
                assertThat(admission.retainedBytes()).isGreaterThan(1).isGreaterThanOrEqualTo(serializer.toBinary(sent.get()).length);
                assertThat(sent.get().encodedBytes()).isEqualTo(admission.retainedBytes());
            }
            assertThat(admission.retainedBytes()).isZero();
            var tiny = new EntityAdmission(8, 1024, 2, 128, 64, 32768);
            var ticket = tiny.acquire("user:alice", EntityAdmission.Priority.NORMAL, 1);
            var operation = TrackedEntityAsk.ask(kit.system(), Duration.ofSeconds(1), UserCommand.Result.class, (reply, cleanup) ->
                NativeEnvelopeAdmission.send(ticket, bytes -> new UserCommand.Mutate(new UserCommand.Reserve(request), reply, deadline, bytes, cleanup), value -> { throw new AssertionError("Must reject before tell"); }, serializer));
            ticket.releaseAfter(operation.physicalCompletion());
            assertThatThrownBy(() -> operation.logical().toCompletableFuture().join()).hasCauseInstanceOf(TrackedEntityAsk.NotEnqueued.class);
            operation.physicalCompletion().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertThat(tiny.retainedBytes()).isZero();
        } finally { kit.shutdownTestKit(); }
    }
    private static HomeParticipationService.Request request() {
        Instant now = Instant.now(); var call = CallId.create("c001", 1); var op = UUID.randomUUID();
        return new HomeParticipationService.Request(new UserId("alice"), call, op, "a".repeat(64), 1, HomeParticipationService.Phase.PREPARING,
            new HomeParticipationService.Grant("c001", 1, 1, HomeParticipationService.group(call), 1, 1, op, now, now.plusSeconds(5), "UNSIGNED"));
    }
}
