package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MountedNativeContractTest {
  @TempDir Path mount;
  final ObjectMapper json = new ObjectMapper().findAndRegisterModules();

  @Test
  void separateTypedMountIsExplicitlyLocalAndRedactsCredentials() throws Exception {
    var file = write(valid());
    var contract = MountedNativeContract.load(file, "local-minikube", "LOCAL_TEST_ONLY");
    assertThat(contract.cell()).isEqualTo("c001");
    assertThat(contract.identity().issuer()).isEqualTo("LOCAL_TEST_ONLY_ISSUER");
    assertThat(contract.cellDatabase().safetyMax()).isEqualTo(4);
    assertThat(contract.toString()).doesNotContain("TEST_ONLY_PRIVATE_PASSWORD");
    assertThatThrownBy(() -> MountedNativeContract.load(file, "production", "LOCAL_TEST_ONLY"))
        .hasMessage("Invalid mounted native enrollment")
        .hasNoCause();
    assertThatThrownBy(() -> MountedNativeContract.load(file, "local-minikube", ""))
        .hasMessage("Invalid mounted native enrollment")
        .hasNoCause();
  }

  @Test
  void unknownDuplicateMissingNullAndCoercedFieldsFailWithoutExposingInput() throws Exception {
    String valid = json.writeValueAsString(valid());
    for (String invalid :
        List.of(
            valid.substring(0, valid.length() - 1)
                + ",\"password\":\"TEST_ONLY_PRIVATE_PASSWORD\"}",
            valid.substring(0, valid.length() - 1) + ",\"cell\":\"c002\"}",
            valid.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""),
            valid.replace("\"storageEpoch\":1", "\"storageEpoch\":null"),
            valid + "{}")) {
      var file = mount.resolve("provider.json");
      Files.writeString(file, invalid);
      assertThatThrownBy(
              () -> MountedNativeContract.load(file, "local-minikube", "LOCAL_TEST_ONLY"))
          .hasMessage("Invalid mounted native enrollment")
          .hasNoCause();
    }
    var missing = valid();
    missing.remove("identity");
    assertThatThrownBy(
            () -> MountedNativeContract.load(write(missing), "local-minikube", "LOCAL_TEST_ONLY"))
        .hasMessage("Invalid mounted native enrollment");
  }

  @Test
  void pathsTopologyAndInputSizeAreBounded() throws Exception {
    var document = valid();
    ((Map<String, Object>) document.get("tls")).put("certificate", "/tmp/unrelated-private.crt");
    var escaped = write(document);
    assertThatThrownBy(
            () -> MountedNativeContract.load(escaped, "local-minikube", "LOCAL_TEST_ONLY"))
        .hasMessage("Invalid mounted native enrollment");
    document = valid();
    document.put(
        "cells",
        List.of(
            Map.of(
                "cell",
                "c002",
                "host",
                "actor.c002.svc",
                "port",
                8443,
                "tlsAuthority",
                "actor.c002.svc",
                "storageEpoch",
                1)));
    var absent = write(document);
    assertThatThrownBy(
            () -> MountedNativeContract.load(absent, "local-minikube", "LOCAL_TEST_ONLY"))
        .hasMessage("Invalid mounted native enrollment");
    var oversized = mount.resolve("provider.json");
    Files.writeString(oversized, " ".repeat(65537));
    assertThatThrownBy(
            () -> MountedNativeContract.load(oversized, "local-minikube", "LOCAL_TEST_ONLY"))
        .hasMessage("Invalid mounted native enrollment");
  }

  @Test
  void projectedSecretSymlinkIsSupported() throws Exception {
    Path actual = mount.resolve("..data");
    Files.createDirectory(actual);
    Files.writeString(actual.resolve("provider.json"), json.writeValueAsString(valid()));
    var projected = mount.resolve("provider.json");
    Files.createSymbolicLink(projected, Path.of("..data/provider.json"));
    assertThat(MountedNativeContract.load(projected, "local-minikube", "LOCAL_TEST_ONLY").cell())
        .isEqualTo("c001");
  }

  Path write(Map<String, Object> document) throws Exception {
    var path = mount.resolve("provider.json");
    Files.writeString(path, json.writeValueAsString(document));
    return path;
  }

  Map<String, Object> valid() {
    var document = new LinkedHashMap<String, Object>();
    document.put("schemaVersion", 1);
    document.put("qualification", "LOCAL_TEST_ONLY");
    document.put("cell", "c001");
    document.put("storageEpoch", 1);
    document.put("routingEpoch", 1);
    document.put("environment", "local-test-only");
    document.put(
        "identity",
        Map.of(
            "issuer",
            "LOCAL_TEST_ONLY_ISSUER",
            "audience",
            "LOCAL_TEST_ONLY_AUDIENCE",
            "maximumJwtLifetime",
            "PT15M",
            "clockSkew",
            "PT30S",
            "revocationPropagationSLO",
            "PT1S",
            "hardSafetyBound",
            "PT5S",
            "preserveJtiOnRefresh",
            true,
            "highWaterSource",
            "LOCAL_TEST_ONLY_LOG"));
    var database =
        Map.of(
            "jdbcUrl",
            "jdbc:postgresql://postgres.c001.svc:5432/signaling?sslmode=verify-full",
            "username",
            "signaling",
            "password",
            "TEST_ONLY_PRIVATE_PASSWORD",
            "quotas",
            Map.of(
                "CRITICAL",
                2,
                "NORMAL",
                1,
                "OUTBOX",
                1,
                "MAINTENANCE",
                1,
                "RENEWAL",
                1,
                "TERMINATION",
                1,
                "RECOVERY",
                2),
            "safetyMax",
            4,
            "controlMax",
            5);
    document.put("cellDatabase", database);
    document.put("directoryDatabase", database);
    document.put(
        "tls",
        new LinkedHashMap<>(
            Map.of(
                "ca",
                file("ca.crt"),
                "certificate",
                file("pod/leaf.crt"),
                "privateKey",
                file("pod/leaf.key"),
                "remotingKeyStore",
                file("pod/remoting.p12"),
                "remotingTrustStore",
                file("trust.p12"))));
    document.put(
        "sources",
        Map.of(
            "clockUrl",
            "https://source.platform.svc/v1/c001/clock",
            "revocationUrl",
            "https://source.platform.svc/v1/c001/revocations",
            "publicKeys",
            Map.of("source", file("source.pub"))));
    document.put(
        "proofs",
        Map.of(
            "keyId",
            "local",
            "privateKey",
            file("proof.key"),
            "publicKeys",
            Map.of("c001/local", file("proof.pub"))));
    document.put("jwtPublicKeys", Map.of("rsa", file("rsa.pub")));
    document.put(
        "cells",
        List.of(
            Map.of(
                "cell",
                "c001",
                "host",
                "actor.c001.svc",
                "port",
                8443,
                "tlsAuthority",
                "actor.c001.svc",
                "storageEpoch",
                1)));
    document.put(
        "gateways",
        List.of(
            Map.of(
                "cell",
                "c001",
                "gatewayId",
                "local-c001",
                "host",
                "gateway.c001.svc",
                "port",
                9443,
                "tlsAuthority",
                "gateway.c001.svc")));
    document.put("directorySnapshot", file("directory.json"));
    document.put("directorySnapshotSha256", "a".repeat(64));
    return document;
  }

  String file(String name) {
    return mount.resolve(name).toString();
  }
}
