package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.actors.admission.CompletionReceipt;
import io.webrtc.signaling.actors.cluster.ApplicationSerializer;
import io.webrtc.signaling.actors.cluster.CallMessage;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.AuthoritySql;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.HomeParticipationService;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Relay frames are authorized against node-local group ownership and node-local negotiation lanes,
 * so they run on the call's owning actor. A relay that reaches an actor without the group never runs.
 */
class CallActorRelayTest {
    static final ActorTestKit kit = ActorTestKit.create();
    static final CallId CALL = new CallId("c001.e1.00000000-0000-0000-0000-000000000011");
    static final Instant NOW = Instant.now();
    static final AuthoritySql.GroupToken TOKEN =
            new AuthoritySql.GroupToken(
                    "c001", 1, 1, HomeParticipationService.group(CALL), 2, "test-node", new UUID(0, 1));

    @AfterAll
    static void close() {
        kit.shutdownTestKit();
    }

    /** The relay handler of the owning node: records the request and answers when told. */
    static final class Handler implements CallActor.RelayHandler {
        final AtomicInteger invoked = new AtomicInteger();
        final AtomicReference<CallActor.Relay> request = new AtomicReference<>();
        final CompletableFuture<String> logical = new CompletableFuture<>();
        final CompletableFuture<Void> physical = new CompletableFuture<>();

        @Override
        public ActorOperation<String> relay(CallActor.Relay relay, Duration budget) {
            invoked.incrementAndGet();
            request.set(relay);
            return new ActorOperation<>(logical, physical);
        }
    }

    static final CallActorStateMachineTest.Backend backend() {
        return new CallActorStateMachineTest.Backend();
    }

    ActorRef<CallMessage> spawn(AtomicBoolean held, Handler handler) {
        var backend = backend();
        var actor =
                kit.spawn(
                        CallActor.create(
                                CALL,
                                backend,
                                () -> held.get() ? Optional.of(TOKEN) : Optional.empty(),
                                Clock.fixed(NOW, ZoneOffset.UTC),
                                null,
                                call -> {
                                },
                                null,
                                handler));
        backend.hydration.done(Optional.empty());
        return actor;
    }

    static CallActor.Relay relay(ActorRef<CallActor.RelayReply> reply, CompletionReceipt receipt) {
        return new CallActor.Relay(
                CALL, "AAEC", "c001", "gateway", "gw-c001-a", reply, NOW.plusSeconds(1), 512, receipt);
    }

    @Test
    void theOwnerRunsTheRelayAndSignalsThePhysicalReceiptAfterItSettles() {
        var handler = new Handler();
        var actor = spawn(new AtomicBoolean(true), handler);
        var replies = kit.<CallActor.RelayReply>createTestProbe();
        var physical = kit.<CompletionReceipt.PhysicalDone>createTestProbe();
        var receipt = new CompletionReceipt(UUID.randomUUID(), physical.ref());

        actor.tell(relay(replies.ref(), receipt));

        kit.createTestProbe().awaitAssert(() -> {
            assertThat(handler.invoked).hasValue(1);
            return null;
        });
        assertThat(handler.request.get().command()).isEqualTo("AAEC");
        handler.logical.complete("REPLY");
        assertThat(replies.receiveMessage().reply()).isEqualTo("REPLY");
        physical.expectNoMessage(Duration.ofMillis(100));
        handler.physical.complete(null);
        assertThat(physical.receiveMessage().operation()).isEqualTo(receipt.operation());
    }

    @Test
    void anActorWithoutTheGroupRefusesWithoutRunningTheRelay() {
        var handler = new Handler();
        var held = new AtomicBoolean(true);
        var actor = spawn(held, handler);
        var replies = kit.<CallActor.RelayReply>createTestProbe();
        var physical = kit.<CompletionReceipt.PhysicalDone>createTestProbe();
        var receipt = new CompletionReceipt(UUID.randomUUID(), physical.ref());
        // The live incarnation loses the group after it started (an incarnation started without
        // the group retires at once and never sees a relay). Wait for the started incarnation first.
        var started = kit.<java.util.Optional<io.webrtc.signaling.storage.CallSnapshotRepository.Snapshot>>createTestProbe();
        actor.tell(new CallActor.GetSnapshot(started.ref()));
        started.receiveMessage();
        held.set(false);

        actor.tell(relay(replies.ref(), receipt));

        var reply = replies.receiveMessage();
        assertThat(reply.reply()).isNull();
        assertThat(reply.error()).isEqualTo("NOT_OWNER");
        assertThat(physical.receiveMessage().operation()).isEqualTo(receipt.operation());
        assertThat(handler.invoked).hasValue(0);
    }

    @Test
    void aFailedRelayRepliesWithItsErrorAndStillSignalsTheReceipt() {
        var handler = new Handler();
        var actor = spawn(new AtomicBoolean(true), handler);
        var replies = kit.<CallActor.RelayReply>createTestProbe();
        var physical = kit.<CompletionReceipt.PhysicalDone>createTestProbe();

        actor.tell(relay(replies.ref(), new CompletionReceipt(UUID.randomUUID(), physical.ref())));
        kit.createTestProbe().awaitAssert(() -> {
            assertThat(handler.invoked).hasValue(1);
            return null;
        });
        handler.logical.completeExceptionally(new IllegalStateException("boom"));
        handler.physical.complete(null);

        assertThat(replies.receiveMessage().error()).isEqualTo("RELAY_FAILED");
        physical.receiveMessage();
    }

    @Test
    void relayEnvelopesRoundTripThroughTheApplicationSerializer() {
        var serializer = new ApplicationSerializer(Adapter.toClassic(kit.system()));
        var replies = kit.<CallActor.RelayReply>createTestProbe();
        var physical = kit.<CompletionReceipt.PhysicalDone>createTestProbe();
        var request = relay(replies.ref(), new CompletionReceipt(UUID.randomUUID(), physical.ref()));

        assertThat(serializer.fromBinary(serializer.toBinary(request), "v1")).isEqualTo(request);
        var reply = new CallActor.RelayReply("REPLY", null);
        assertThat(serializer.fromBinary(serializer.toBinary(reply), "v1")).isEqualTo(reply);
    }
}
