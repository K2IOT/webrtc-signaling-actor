package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import java.net.InetAddress;
import java.time.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class EdgeAdmissionTest {
  @Test
  void handshakeAndPerIpBudgetsRejectBeforeTlsAndRetainedIpStateStaysBounded() throws Exception {
    var nanos = new AtomicLong();
    var edge =
        new EdgeAdmission(new EdgeAdmission.Limits(2, 2, 1, 2, Duration.ofSeconds(1)), nanos::get);
    var a = InetAddress.getByName("192.0.2.1");
    var b = InetAddress.getByName("192.0.2.2");
    var c = InetAddress.getByName("192.0.2.3");
    var first = edge.acquire(a);
    assertThatThrownBy(() -> edge.acquire(a)).isInstanceOf(EdgeAdmission.Rejected.class);
    var second = edge.acquire(b);
    assertThatThrownBy(() -> edge.acquire(c)).isInstanceOf(EdgeAdmission.Rejected.class);
    first.close();
    second.close();
    var retry = edge.acquire(a);
    retry.close();
    assertThatThrownBy(() -> edge.acquire(a)).isInstanceOf(EdgeAdmission.Rejected.class);
    assertThat(edge.trackedIps()).isEqualTo(2);
    nanos.addAndGet(Duration.ofSeconds(2).toNanos());
    try (var next = edge.acquire(c)) {
      assertThat(edge.trackedIps()).isLessThanOrEqualTo(2);
      assertThat(edge.activeHandshakes()).isEqualTo(1);
    }
    assertThat(edge.activeHandshakes()).isZero();
  }

  @Test
  void heartbeatAdvanceInspectsOnlyElapsedSlotsAndRetainsFutureTimers() {
    var fixture = new GatewayProtocolIT();
    var channel = fixture.channel();
    try (var wheel = new HeartbeatWheel(fixture.clock, 10)) {
      fixture.authenticate(channel);
      ((io.netty.handler.codec.http.websocketx.TextWebSocketFrame) channel.readOutbound())
          .release();
      wheel.register(fixture.registry.binding(channel.attr(ConnectionRegistry.CONNECTION).get()));
      assertThat(wheel.advance()).isZero();
      assertThat(wheel.retainedEvents()).isEqualTo(3);
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
