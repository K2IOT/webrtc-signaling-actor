package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class BootRegistryTest {

  private static final String BASE = "spiffe://signaling/local-test-only/cell/cell-a/";

  private final BootRegistry registry = new BootRegistry();

  private static PeerIdentity peer(String plane) {
    return new PeerIdentity(BASE + plane, "cell-a", plane, null);
  }

  @Test
  void firstRegistrationSucceeds() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();

    BootRegistry.Registration registration = registry.register(peer("actor"), pod, boot);

    assertThat(registration).isEqualTo(new BootRegistry.Registration(peer("actor"), pod, boot));
  }

  @Test
  void sameBootAgainSucceeds() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();
    registry.register(peer("actor"), pod, boot);

    assertThat(registry.register(peer("actor"), pod, boot).boot()).isEqualTo(boot);
  }

  @Test
  void newBootReplacesOld() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID first = UUID.randomUUID();
    UUID second = UUID.randomUUID();
    registry.register(peer("actor"), pod, first);

    assertThat(registry.register(peer("actor"), pod, second).boot()).isEqualTo(second);
    assertThat(registry.register(peer("actor"), pod, second).boot()).isEqualTo(second);
  }

  @Test
  void staleBootAfterNewerIsRejected() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID first = UUID.randomUUID();
    registry.register(peer("actor"), pod, first);
    registry.register(peer("actor"), pod, UUID.randomUUID());

    assertThatThrownBy(() -> registry.register(peer("actor"), pod, first))
        .isInstanceOf(BootRegistry.BootRejected.class)
        .hasMessage(null);
  }

  @Test
  void podBoundToOtherIdentityIsRejected() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID boot = UUID.randomUUID();
    registry.register(peer("actor"), pod, boot);

    assertThatThrownBy(() -> registry.register(peer("gateway"), pod, boot))
        .isInstanceOf(BootRegistry.BootRejected.class);
    assertThatThrownBy(() -> registry.register(peer("gateway"), pod, UUID.randomUUID()))
        .isInstanceOf(BootRegistry.BootRejected.class);
    // The legitimate holder is unaffected by the rejected attempts.
    assertThat(registry.register(peer("actor"), pod, boot).boot()).isEqualTo(boot);
  }

  @Test
  void sameSpiffeIdWithDifferentFieldsIsStillTheSameIdentity() throws Exception {
    UUID pod = UUID.randomUUID();
    registry.register(peer("actor"), pod, UUID.randomUUID());

    assertThatThrownBy(
            () ->
                registry.register(
                    new PeerIdentity(peer("gateway").spiffeId(), "cell-a", "actor", null),
                    pod,
                    UUID.randomUUID()))
        .isInstanceOf(BootRegistry.BootRejected.class);
  }

  @Test
  void registryIsBounded() throws Exception {
    for (int i = 0; i < BootRegistry.MAX_PODS; i++) {
      registry.register(peer("actor"), UUID.randomUUID(), UUID.randomUUID());
    }

    assertThatThrownBy(() -> registry.register(peer("actor"), UUID.randomUUID(), UUID.randomUUID()))
        .isInstanceOf(BootRegistry.BootRejected.class);
  }

  @Test
  void knownPodStillRegistersWhenFull() throws Exception {
    UUID pod = UUID.randomUUID();
    registry.register(peer("actor"), pod, UUID.randomUUID());
    for (int i = 1; i < BootRegistry.MAX_PODS; i++) {
      registry.register(peer("actor"), UUID.randomUUID(), UUID.randomUUID());
    }
    UUID next = UUID.randomUUID();

    assertThat(registry.register(peer("actor"), pod, next).boot()).isEqualTo(next);
  }

  @Test
  void recentRetiredBootsStayRejected() throws Exception {
    UUID pod = UUID.randomUUID();
    UUID[] boots = new UUID[BootRegistry.RETIRED_BOOTS_PER_POD + 1];
    for (int i = 0; i < boots.length; i++) {
      boots[i] = UUID.randomUUID();
      registry.register(peer("actor"), pod, boots[i]);
    }
    registry.register(peer("actor"), pod, UUID.randomUUID());

    // boots[1..8] are the last 8 retired boots and stay rejected.
    for (int i = 1; i < boots.length; i++) {
      UUID stale = boots[i];
      assertThatThrownBy(() -> registry.register(peer("actor"), pod, stale))
          .isInstanceOf(BootRegistry.BootRejected.class);
    }
    // boots[0] fell out of the bounded window and is no longer remembered.
    assertThat(registry.register(peer("actor"), pod, boots[0]).boot()).isEqualTo(boots[0]);
  }

  @Test
  void nullArgumentsRejected() {
    assertThatThrownBy(() -> registry.register(null, UUID.randomUUID(), UUID.randomUUID()))
        .isInstanceOf(BootRegistry.BootRejected.class);
    assertThatThrownBy(() -> registry.register(peer("actor"), null, UUID.randomUUID()))
        .isInstanceOf(BootRegistry.BootRejected.class);
    assertThatThrownBy(() -> registry.register(peer("actor"), UUID.randomUUID(), null))
        .isInstanceOf(BootRegistry.BootRejected.class);
  }
}
