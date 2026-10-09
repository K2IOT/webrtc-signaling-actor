package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class GatewayBootControllerTest {
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
}
