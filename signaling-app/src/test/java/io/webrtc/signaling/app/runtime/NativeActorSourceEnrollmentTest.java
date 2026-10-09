package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.IdentitySecurityContract;
import java.net.URI;
import java.security.*;
import java.time.Duration;
import java.util.*;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

class NativeActorSourceEnrollmentTest {
  private NativeActorSourceEnrollment.Endpoint endpoint(String uri, Map<String, PublicKey> keys)
      throws Exception {
    return new NativeActorSourceEnrollment.Endpoint(
        URI.create(uri), SSLContext.getInstance("TLSv1.3"), keys);
  }

  private IdentitySecurityContract identity(Duration hard) {
    return new IdentitySecurityContract(
        "TEST_ONLY_ISSUER",
        "TEST_ONLY_AUDIENCE",
        Duration.ofMinutes(10),
        Duration.ofSeconds(30),
        hard.dividedBy(2),
        hard,
        true,
        "TEST_ONLY_SOURCE");
  }

  @Test
  void pinnedSourceKeysAreCopiedBeforeTheCallerCanChangeTrust() throws Exception {
    var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
    var mutable = new HashMap<String, PublicKey>();
    mutable.put("TEST_ONLY_SOURCE", key);
    var source = endpoint("https://source.example/v1/clock", mutable);
    mutable.clear();
    assertThat(source.keys()).containsEntry("TEST_ONLY_SOURCE", key);
    assertThatThrownBy(() -> source.keys().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void endpointCannotEnrollCleartextRedirectCredentialsOrAmbiguousPaths() throws Exception {
    var keys =
        Map.of(
            "TEST_ONLY_SOURCE",
            KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic());
    for (String uri :
        List.of(
            "http://source.example/v1/clock",
            "https://private:credential@source.example/v1/clock",
            "https://source.example/v1/clock?token=private",
            "https://source.example/a/../clock"))
      assertThatThrownBy(() -> endpoint(uri, keys)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidCellEpochOrMissingProcessIdentityCannotEnroll() throws Exception {
    var source =
        endpoint(
            "https://source.example/v1/clock",
            Map.of(
                "TEST_ONLY_SOURCE",
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic()));
    var identity = identity(Duration.ofSeconds(1));
    UUID pod = UUID.randomUUID(), boot = UUID.randomUUID();
    assertThatThrownBy(
            () ->
                new NativeActorSourceEnrollment("bad cell", 1, pod, boot, identity, source, source))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new NativeActorSourceEnrollment("c001", 0, pod, boot, identity, source, source))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new NativeActorSourceEnrollment("c001", 1, null, boot, identity, source, source))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void revocationBoundMustPermitTheFixedHundredMillisecondPollMargin() throws Exception {
    var source =
        endpoint(
            "https://source.example/v1/clock",
            Map.of(
                "TEST_ONLY_SOURCE",
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic()));
    assertThatThrownBy(
            () ->
                new NativeActorSourceEnrollment(
                    "c001",
                    1,
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    identity(Duration.ofMillis(199)),
                    source,
                    source))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(
            new NativeActorSourceEnrollment(
                "c001",
                1,
                UUID.randomUUID(),
                UUID.randomUUID(),
                identity(Duration.ofMillis(200)),
                source,
                source))
        .isNotNull();
  }
}
