package io.webrtc.signaling.gateway;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.internal.SessionReply;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.rpc.RpcBusinessHandler;
import io.webrtc.signaling.storage.GatewayLeaseRepository;
import io.webrtc.signaling.storage.SessionRegistryService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayBootControllerTest {
    static SessionReply reply(NativeSessionHandler.Request request, long remaining) {
        var id = request.gateway();
        var boot =
                new GatewayLeaseRepository.Boot(
                        id.gatewayId(),
                        id.bootId(),
                        id.storageEpoch(),
                        id.region(),
                        id.cell(),
                        request.renewalSequence(),
                        Instant.now().plusSeconds(15),
                        request.operation());
        return SessionReply.newBuilder()
                .setOperationId(request.operation().toString())
                .setAckCommitted(true)
                .setStatus("COMMITTED")
                .setResult(
                        ByteString.copyFrom(
                                RpcBusinessHandler.encode(new SessionRegistryService.BootGrant(boot, remaining))))
                .build();
    }

    @Test
    void conservativeBootDeadlineStartsAtRequestAndLatePulseCannotResurrectExpiredBoot() {
        var now = new AtomicLong(1000000000L);
        var replies = new ArrayDeque<CompletableFuture<SessionReply>>();
        var sent = new ArrayList<NativeSessionHandler.Request>();
        var identity =
                new NativeSessionHandler.GatewayIdentity(
                        "gw", UUID.randomUUID(), "c001", 1, "TEST_ONLY_REGION");
        var controller =
                new GatewayBootController(
                        identity,
                        (request, budget) -> {
                            sent.add(request);
                            var future = new CompletableFuture<SessionReply>();
                            replies.add(future);
                            return future;
                        },
                        now::get);
        try {
            controller.start();
            assertThat(controller.current()).isFalse();
            var start = sent.getFirst();
            now.addAndGet(Duration.ofSeconds(2).toNanos());
            replies.remove().complete(reply(start, 15000));
            assertThat(controller.current()).isTrue();
            now.set(8000000000L);
            controller.pulse();
            assertThat(sent.getLast().renewalSequence()).isEqualTo(2);
            now.set(12000000000L);
            assertThat(controller.current()).isFalse();
            replies.remove().complete(reply(sent.getLast(), 15000));
            assertThat(controller.current()).isFalse();
            controller.pulse();
            assertThat(sent).hasSize(2);
        } finally {
            controller.close();
        }
    }

    @Test
    void unknownStartNeverEnablesIngressOrRetriesSameBoot() {
        var sent = new AtomicInteger();
        var identity =
                new NativeSessionHandler.GatewayIdentity(
                        "gw", UUID.randomUUID(), "c001", 1, "TEST_ONLY_REGION");
        try (var controller =
                     new GatewayBootController(
                             identity,
                             (request, budget) -> {
                                 sent.incrementAndGet();
                                 return CompletableFuture.completedFuture(
                                         SessionReply.newBuilder()
                                                 .setOperationId(request.operation().toString())
                                                 .setErrorCode("OUTCOME_UNKNOWN")
                                                 .build());
                             },
                             System::nanoTime)) {
            controller.start();
            controller.start();
            controller.pulse();
            assertThat(controller.current()).isFalse();
            assertThat(sent).hasValue(1);
        }
    }

    @Test
    void capabilityBootstrapWaitsWithoutConsumingBootAndLostBootIsNeverRetried() throws Exception {
        var ready = new AtomicBoolean();
        var checks = new AtomicInteger();
        var sent = new AtomicInteger();
        var identity =
                new NativeSessionHandler.GatewayIdentity(
                        "gw", UUID.randomUUID(), "c001", 1, "TEST_ONLY_REGION");
        try (var controller =
                     new GatewayBootController(
                             identity,
                             (request, budget) -> {
                                 sent.incrementAndGet();
                                 return CompletableFuture.completedFuture(
                                         SessionReply.newBuilder().setErrorCode("OUTCOME_UNKNOWN").build());
                             },
                             System::nanoTime)) {
            controller.startWhen(
                    () -> {
                        checks.incrementAndGet();
                        return ready.get();
                    });
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> checks.get() > 0);
            assertThat(sent).hasValue(0);
            assertThat(controller.current()).isFalse();
            ready.set(true);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> sent.get() == 1);
            controller.startWhen(() -> true);
            controller.start();
            controller.pulse();
            assertThat(sent).hasValue(1);
            assertThat(controller.current()).isFalse();
        }
    }
}
