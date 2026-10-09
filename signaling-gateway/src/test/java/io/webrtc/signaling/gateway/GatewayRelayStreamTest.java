package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.SessionRepository;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Real Netty lifecycle with TEST_ONLY route/security and workload enrollment, not native delivery
 * evidence.
 */
class GatewayRelayStreamTest {
  static final class Fixture implements AutoCloseable {
    final Clock clock;
    final AtomicBoolean clockTrusted = new AtomicBoolean(true);
    final UUID boot = UUID.randomUUID();
    final AtomicBoolean healthy = new AtomicBoolean(true);
    final ConnectionRegistry registry = new ConnectionRegistry("TEST_ONLY_GW", boot, 8, 16);
    final DeliveryCreditController outbound = new DeliveryCreditController(64, 1048576, 8, 65536);
    final RpcAdmission admission = new RpcAdmission(1, 98304, 1, 98304);
    final EmbeddedChannel channel;
    final AuthenticatedSession recipient;
    final GatewayRelayStream stream;

    Fixture(boolean blocked) {
      this(blocked, Runnable::run, Clock.systemUTC());
    }

    Fixture(boolean blocked, Executor cpu, Clock clock) {
      this.clock = clock;
      channel =
          blocked
              ? new EmbeddedChannel(transport, new OutboundAdmissionHandler(outbound))
              : new EmbeddedChannel(new OutboundAdmissionHandler(outbound));
      UUID connection = registry.attach(channel);
      var principal =
          new AuthPrincipal(
              new UserId("TEST_ONLY_RECIPIENT"),
              new SessionKey("TEST_ONLY_ISSUER", "TEST_ONLY_JTI"),
              Instant.now().plusSeconds(60),
              Instant.now(),
              "TEST_ONLY_KEY",
              1);
      var route =
          new SessionRepository.Route(
              principal.userId(),
              principal.key(),
              new SessionIncarnation(UUID.randomUUID()),
              1,
              "TEST_ONLY_GW",
              boot,
              connection,
              principal.expiresAt(),
              principal.signingKeyId(),
              1);
      assertThat(registry.bind(connection, route, principal)).isTrue();
      recipient =
          new AuthenticatedSession(route.user(), route.key(), route.incarnation(), 1, connection);
      var services =
          new GatewayServices() {
            public boolean currentBoot() {
              return healthy.get();
            }

            public AuthorizationStatus cachedSecurity(AuthPrincipal p, Instant now) {
              return healthy.get() ? AuthorizationStatus.ALLOWED : AuthorizationStatus.REVOKED;
            }

            public CompletionStage<AuthPrincipal> verify(String t, Instant now) {
              throw new AssertionError();
            }

            public CompletionStage<SessionRepository.Route> register(
                AuthPrincipal p, UUID c, Duration b) {
              throw new AssertionError();
            }

            public CompletionStage<SessionRepository.Route> refresh(
                SessionRepository.Route r, AuthPrincipal p, Duration b) {
              throw new AssertionError();
            }

            public CompletionStage<Void> close(SessionRepository.Route r) {
              throw new AssertionError();
            }

            public CompletionStage<String> command(CallCommand c, Duration b) {
              throw new AssertionError();
            }
          };
      stream = new GatewayRelayStream(registry, services, clock, cpu, admission, clockTrusted::get);
    }

    final OutboundRelayReceiptTest.UnknownCloseTransport transport =
        new OutboundRelayReceiptTest.UnknownCloseTransport();

    RelayDelivery delivery(SignalEnvelope.Type type) {
      var call = CallId.create("c001", 1);
      var sender =
          new AuthenticatedSession(
              new UserId("TEST_ONLY_SENDER"),
              new SessionKey("TEST_ONLY_ISSUER", "TEST_ONLY_SENDER_JTI"),
              new SessionIncarnation(UUID.randomUUID()),
              1,
              UUID.randomUUID());
      return new RelayDelivery(
          new CallCommand(
              type,
              sender,
              new RequestId(UUID.randomUUID()),
              call,
              CommandScope.call(call),
              null,
              new NegotiationId(17),
              new IceGeneration(29),
              "{\"sdp\":\"" + "x".repeat(16384) + "\"}",
              "a".repeat(64)),
          recipient,
          7,
          clock.instant().plusSeconds(2));
    }

    RpcOperation<RelayWriteReceipt> send(RelayDelivery delivery, Duration budget) {
      return stream.send(
          delivery, new CellRpcServer.Peer("c001", "actor", "TEST_ONLY_ACTOR"), budget);
    }

    public void close() {
      transport.settled();
      channel.finishAndReleaseAll();
    }
  }

  static final class MutableClock extends Clock {
    Instant value = Instant.now();

    public java.time.ZoneId getZone() {
      return java.time.ZoneOffset.UTC;
    }

    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    public Instant instant() {
      return value;
    }
  }

  @Test
  void queuedFrameCannotRenewItsOriginalAuthorizationWindow() {
    var queued = new java.util.concurrent.atomic.AtomicReference<Runnable>();
    var clock = new MutableClock();
    try (var f = new Fixture(false, queued::set, clock)) {
      var delivery = f.delivery(SignalEnvelope.Type.OFFER);
      var result = f.send(delivery, Duration.ofSeconds(1));
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);
      clock.value = clock.value.plusSeconds(3);
      queued.get().run();
      assertThat(result.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();
      assertThat((Object) f.channel.readOutbound()).isNull();
    }
  }

  @Test
  void clockLossAndExpiredOrUnboundedSourceAuthorityCannotWrite() {
    try (var f = new Fixture(false)) {
      var delivery = f.delivery(SignalEnvelope.Type.OFFER);
      for (var until :
          List.of(
              Instant.now().minusSeconds(1),
              Instant.now().plusMillis(100),
              Instant.now().plusSeconds(6))) {
        var invalid =
            new RelayDelivery(
                delivery.command(), delivery.recipient(), delivery.callVersion(), until);
        var result = f.send(invalid, Duration.ofSeconds(1));
        assertThat(result.logical().toCompletableFuture()).isCompletedExceptionally();
        assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
      }
      f.clockTrusted.set(false);
      var rejected = f.send(delivery, Duration.ofSeconds(1));
      assertThat(rejected.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(rejected.physicalCompletion().toCompletableFuture()).isDone();
      assertThat((Object) f.channel.readOutbound()).isNull();
    }
  }

  @Test
  void largeSdpUsesVolatileFrameAndExactBoundRecipient() {
    try (var f = new Fixture(false)) {
      var delivery = f.delivery(SignalEnvelope.Type.OFFER);
      var result = f.send(delivery, Duration.ofSeconds(1));
      var receipt = result.logical().toCompletableFuture().join();
      assertThat(receipt.matches(delivery.command())).isTrue();
      assertThat(receipt.callVersion()).isEqualTo(7);
      assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();
      var frame = (TextWebSocketFrame) f.channel.readOutbound();
      try {
        assertThat(frame).isInstanceOf(OutboundAdmissionHandler.RelayFrame.class);
        var json = new ObjectMapper().readTree(frame.text());
        assertThat(json.path("type").asText()).isEqualTo("OFFER");
        assertThat(json.path("negotiationId").asText()).isEqualTo("17");
        assertThat(json.path("iceGeneration").asText()).isEqualTo("29");
        assertThat(json.path("sessionIncarnation").asText())
            .isEqualTo(f.recipient.incarnation().value().toString());
        assertThat(json.path("payload").path("sdp").asText()).hasSize(16384);
      } catch (java.io.IOException invalid) {
        throw new AssertionError(invalid);
      } finally {
        ReferenceCountUtil.release(frame);
      }
    }
  }

  @Test
  void logicalDeadlineKeepsCreditUntilOriginalStartedWriteActuallyEnds() throws Exception {
    try (var f = new Fixture(true)) {
      var delivery = f.delivery(SignalEnvelope.Type.OFFER);
      var result = f.send(delivery, Duration.ofMillis(40));
      assertThatThrownBy(
              () -> result.logical().toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
          .hasCauseInstanceOf(TimeoutException.class);
      assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);
      var rejected = f.send(delivery, Duration.ofSeconds(1));
      assertThat(rejected.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(rejected.physicalCompletion().toCompletableFuture()).isDone();
      f.channel.close();
      assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();
      f.transport.settled();
      assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();
    }
  }

  @Test
  void staleLocalAuthorizationAndWrongCoordinatorNeverWrite() {
    try (var f = new Fixture(false)) {
      var delivery = f.delivery(SignalEnvelope.Type.OFFER);
      var bad =
          f.stream.send(
              delivery,
              new CellRpcServer.Peer("c002", "actor", "TEST_ONLY_ACTOR"),
              Duration.ofSeconds(1));
      assertThat(bad.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(bad.physicalCompletion().toCompletableFuture()).isDone();
      f.healthy.set(false);
      var stale = f.send(delivery, Duration.ofSeconds(1));
      assertThat(stale.logical().toCompletableFuture()).isCompletedExceptionally();
      assertThat(stale.physicalCompletion().toCompletableFuture()).isDone();
      assertThat((Object) f.channel.readOutbound()).isNull();
      assertThat(f.admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();
    }
  }
}
