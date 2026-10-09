package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** TEST_ONLY proof source; checks actual adapter/cache wire composition, never delivery success. */
class NativeGatewayRelayProofReuseTest {
  @Test
  void warmFramesReuseOneVerifiedHomeReadAndStillKeepOriginalFrameIdentity() throws Exception {
    var fixture = new NativeRelaySessionProofCacheTest();
    var codec = fixture.codec();
    var reads = new AtomicInteger();
    var relays = new AtomicInteger();
    var network =
        new NativeGatewayCommands.TrackedNetwork() {
          public RpcOperation<SessionReply> sessionTracked(SessionCommand wire, Duration budget) {
            reads.incrementAndGet();
            try {
              var r =
                  new com.fasterxml.jackson.databind.ObjectMapper()
                      .findAndRegisterModules()
                      .readValue(
                          wire.getPayload().toByteArray(), NativeSessionHandler.Request.class);
              assertThat(r.type()).isEqualTo("READ_RELAY_PROOF");
              assertThat(r.proofCommand().payloadJson()).isEqualTo("{}");
              var signed = fixture.issue(codec, r.proofCommand());
              return new RpcOperation<>(
                  CompletableFuture.completedFuture(
                      SessionReply.newBuilder()
                          .setOperationId(wire.getOperationId())
                          .setStatus("READ")
                          .setResult(ByteString.copyFrom(RpcBusinessHandler.encode(signed)))
                          .build()),
                  CompletableFuture.completedFuture(null));
            } catch (java.io.IOException invalid) {
              throw new AssertionError(invalid);
            }
          }

          public CompletionStage<InternalReply> call(
              CellRpcServer.Operation operation, InternalCommand wire, Duration budget) {
            relays.incrementAndGet();
            assertThat(operation).isEqualTo(CellRpcServer.Operation.RELAY);
            return CompletableFuture.completedFuture(
                InternalReply.newBuilder()
                    .setOperationId(wire.getOperationId())
                    .setCallId(wire.getCallId())
                    .setErrorCode("UNSUPPORTED_OPERATION")
                    .build());
          }
        };
    var cache =
        new NativeRelaySessionProofCache(
            2, 1, fixture.clock, fixture.ticks::get, fixture.healthy::get, codec);
    var gateway =
        new NativeSessionHandler.GatewayIdentity(
            fixture.route.gatewayId(), fixture.route.bootId(), "c001", 1, "TEST_ONLY");
    var commands =
        new NativeGatewayCommands(gateway, u -> fixture.home, network, fixture.clock)
            .relayProofCache(cache);
    for (int i = 0; i < 3; i++) {
      var command = fixture.command(1);
      String reply =
          commands
              .execute(command, fixture.route, "TEST_ONLY_ORIGINAL_TOKEN", Duration.ofSeconds(1))
              .toCompletableFuture()
              .join();
      assertThat(reply).contains(command.requestId().value().toString(), "UNSUPPORTED_OPERATION");
    }
    assertThat(reads).hasValue(1);
    assertThat(relays).hasValue(3);
    fixture.healthy.set(false);
    String rejected =
        commands
            .execute(
                fixture.command(1),
                fixture.route,
                "TEST_ONLY_ORIGINAL_TOKEN",
                Duration.ofSeconds(1))
            .toCompletableFuture()
            .join();
    assertThat(rejected).contains("OUTCOME_UNKNOWN");
    assertThat(reads).hasValue(1);
    assertThat(relays).hasValue(3);
    assertThat(cache.retainedBytes())
        .isLessThanOrEqualTo(2L * NativeRelaySessionProofCache.ENTRY_BYTES);
  }

  @Test
  void cacheInstallationRequiresOriginalTrackedSessionReceipt() throws Exception {
    var fixture = new NativeRelaySessionProofCacheTest();
    var codec = fixture.codec();
    var network =
        new NativeGatewayCommands.Network() {
          public CompletionStage<SessionReply> session(SessionCommand c, Duration b) {
            throw new AssertionError();
          }

          public CompletionStage<InternalReply> call(
              CellRpcServer.Operation o, InternalCommand c, Duration b) {
            throw new AssertionError();
          }
        };
    var gateway =
        new NativeSessionHandler.GatewayIdentity(
            fixture.route.gatewayId(), fixture.route.bootId(), "c001", 1, "TEST_ONLY");
    var commands = new NativeGatewayCommands(gateway, u -> fixture.home, network, fixture.clock);
    assertThatThrownBy(
            () ->
                commands.relayProofCache(
                    new NativeRelaySessionProofCache(
                        1, 1, fixture.clock, fixture.ticks::get, fixture.healthy::get, codec)))
        .isInstanceOf(IllegalStateException.class);
  }
}
