package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity;
import io.webrtc.signaling.protocol.Identity.SessionIncarnation;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.SignalEnvelope;
import io.webrtc.signaling.storage.SessionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UserRateLimitHandlerTest {
  private final UUID boot = UUID.randomUUID();
  private final ConnectionRegistry registry = new ConnectionRegistry("TEST_ONLY_GW", boot, 8, 16);
  private final AtomicLong nanos = new AtomicLong();
  private final UserRateLimiter limiter =
      new UserRateLimiter(new GatewayRateLimits(20, 40, 30, 60, 1, 2, 8), nanos::get);
  private final List<EmbeddedChannel> channels = new ArrayList<>();

  @AfterEach
  void close() {
    channels.forEach(EmbeddedChannel::finishAndReleaseAll);
  }

  private EmbeddedChannel socket(String user, String jti) {
    var channel =
        new EmbeddedChannel(new UserRateLimitHandler(registry, limiter, Clock.systemUTC()));
    channels.add(channel);
    UUID connection = registry.attach(channel);
    var principal =
        new AuthPrincipal(
            new UserId(user),
            new SessionKey("TEST_ONLY_ISSUER", jti),
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
    return channel;
  }

  private static FrameAdmissionHandler.Admitted frame(
      SignalEnvelope.Type type, AtomicInteger released) {
    var envelope =
        new SignalEnvelope(
            1, type, new Identity.RequestId(UUID.randomUUID()), null, null, null, "{}");
    return new FrameAdmissionHandler.Admitted(envelope, 10, 0, released::incrementAndGet);
  }

  @Test
  void deniedFrameIsReleasedAndAnsweredWithTypedRateLimitedError() {
    var released = new AtomicInteger();
    var channel = socket("TEST_ONLY_U1", "j1");
    channel.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    channel.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    assertThat((Object) channel.readInbound()).isNotNull();
    assertThat((Object) channel.readInbound()).isNotNull();
    channel.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    assertThat((Object) channel.readInbound()).isNull();
    assertThat(released).hasValue(1);
    var reply = (TextWebSocketFrame) channel.readOutbound();
    try {
      assertThat(reply.text()).contains("\"type\":\"ERROR\"").contains("\"code\":\"RATE_LIMITED\"");
      assertThat(reply.text()).contains("\"retryAfterMs\":").contains("\"ackCommitted\":false");
    } finally {
      reply.release();
    }
    assertThat(channel.isActive()).isTrue();
  }

  @Test
  void sessionsOfOneUserShareASingleBucket() {
    var released = new AtomicInteger();
    var first = socket("TEST_ONLY_U1", "j1");
    var second = socket("TEST_ONLY_U1", "j2");
    first.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    second.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    second.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    assertThat((Object) second.readInbound()).isNotNull();
    assertThat((Object) second.readInbound()).isNull();
    assertThat((Object) second.readOutbound()).isNotNull();
    assertThat(released).hasValue(1);
  }

  @Test
  void criticalCommandsAreNeverRateLimitedByTheUserBucket() {
    var released = new AtomicInteger();
    var channel = socket("TEST_ONLY_U1", "j1");
    for (int i = 0; i < 6; i++) channel.writeInbound(frame(SignalEnvelope.Type.HANGUP, released));
    int passed = 0;
    while (channel.readInbound() != null) passed++;
    assertThat(passed).isEqualTo(6);
    assertThat(released).hasValue(0);
    assertThat((Object) channel.readOutbound()).isNull();
  }

  @Test
  void unauthenticatedSocketsPassThroughToTheAuthHandler() {
    var released = new AtomicInteger();
    var channel =
        new EmbeddedChannel(new UserRateLimitHandler(registry, limiter, Clock.systemUTC()));
    channels.add(channel);
    registry.attach(channel);
    for (int i = 0; i < 5; i++) channel.writeInbound(frame(SignalEnvelope.Type.AUTH, released));
    int passed = 0;
    while (channel.readInbound() != null) passed++;
    assertThat(passed).isEqualTo(5);
  }

  @Test
  void differentUsersAreNotAffected() {
    var released = new AtomicInteger();
    var a = socket("TEST_ONLY_A", "ja");
    var b = socket("TEST_ONLY_B", "jb");
    for (int i = 0; i < 3; i++) a.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    b.writeInbound(frame(SignalEnvelope.Type.INVITE, released));
    assertThat((Object) b.readInbound()).isNotNull();
  }
}
