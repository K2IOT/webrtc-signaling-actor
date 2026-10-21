package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Uses the TEST_ONLY certificates under {@code src/test/resources/pki}. */
class PeerIdentityTest {

  private static final String BASE = "spiffe://signaling/local-test-only";
  private static final Set<String> CELLS = Set.of("cell-a", "cell-b");

  static X509Certificate load(String name) {
    try (InputStream in = PeerIdentityTest.class.getResourceAsStream("/pki/" + name + ".pem")) {
      if (in == null) {
        throw new IllegalStateException("missing fixture " + name);
      }
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
    } catch (java.io.IOException | java.security.cert.CertificateException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void assertWorkloadRejected(String fixture) {
    assertThatThrownBy(() -> PeerIdentity.fromCertificate(load(fixture), CELLS))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static void assertAdminRejected(String fixture) {
    assertThatThrownBy(() -> PeerIdentity.admin(load(fixture)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void actorUriSanParses() {
    PeerIdentity peer = PeerIdentity.fromCertificate(load("actor-cell-a"), CELLS);

    assertThat(peer)
        .isEqualTo(new PeerIdentity(BASE + "/cell/cell-a/actor", "cell-a", "actor", null));
  }

  @Test
  void gatewayWithInstanceParses() {
    PeerIdentity peer = PeerIdentity.fromCertificate(load("gateway-cell-a-gw-1"), CELLS);

    assertThat(peer)
        .isEqualTo(
            new PeerIdentity(BASE + "/cell/cell-a/gateway/gw-1", "cell-a", "gateway", "gw-1"));
  }

  @Test
  void controlParses() {
    PeerIdentity peer = PeerIdentity.fromCertificate(load("control-cell-b"), CELLS);

    assertThat(peer.cell()).isEqualTo("cell-b");
    assertThat(peer.plane()).isEqualTo("control");
    assertThat(peer.instance()).isNull();
  }

  @Test
  void otherSanTypesNextToOneUriSanAreIgnored() {
    assertThat(PeerIdentity.fromCertificate(load("dns-and-actor-uri"), CELLS).plane())
        .isEqualTo("actor");
  }

  @Test
  void noUriSanRejected() {
    assertWorkloadRejected("no-uri-san");
    assertWorkloadRejected("no-san");
  }

  @Test
  void twoUriSansRejected() {
    assertWorkloadRejected("two-uri-sans");
  }

  @Test
  void foreignTrustDomainRejected() {
    assertWorkloadRejected("foreign-trust-domain");
  }

  @Test
  void unknownCellRejected() {
    assertWorkloadRejected("unknown-cell");
    assertThatThrownBy(() -> PeerIdentity.fromCertificate(load("actor-cell-a"), Set.of("cell-b")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedPathShapesRejected() {
    assertWorkloadRejected("unknown-plane");
    assertWorkloadRejected("extra-path-segment");
    assertWorkloadRejected("trailing-slash");
    assertWorkloadRejected("uppercase-instance");
  }

  @Test
  void adminPathOnWorkloadParserRejected() {
    assertWorkloadRejected("admin-ops");
    assertWorkloadRejected("admin-path-on-workload-ca");
  }

  @Test
  void workloadPathOnAdminParserRejected() {
    assertAdminRejected("actor-cell-a");
    assertAdminRejected("workload-path-on-admin-ca");
  }

  @Test
  void adminParses() {
    assertThat(PeerIdentity.admin(load("admin-ops"))).isEqualTo("ops");
    assertThat(PeerIdentity.admin(load("admin-path-on-workload-ca"))).isEqualTo("ops");
  }

  @Test
  void malformedAdminCertificatesRejected() {
    assertAdminRejected("admin-two-uri-sans");
    assertAdminRejected("admin-no-uri-san");
    assertAdminRejected("admin-uppercase-name");
    assertAdminRejected("admin-name-too-long");
    assertAdminRejected("admin-extra-segment");
    assertAdminRejected("admin-foreign-domain");
  }

  @Test
  void rejectionsCarryNoDetail() {
    assertThatThrownBy(() -> PeerIdentity.fromCertificate(load("unknown-cell"), CELLS))
        .satisfies(
            e -> {
              assertThat(e.getMessage()).isNull();
              assertThat(e.getCause()).isNull();
            });
    assertThatThrownBy(() -> PeerIdentity.admin(load("admin-uppercase-name")))
        .satisfies(e -> assertThat(e.getMessage()).isNull());
  }

  @Test
  void nullArgumentsRejected() {
    assertThatThrownBy(() -> PeerIdentity.fromCertificate(null, CELLS))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> PeerIdentity.admin(null)).isInstanceOf(IllegalArgumentException.class);
  }
}
