package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.auth.IdentitySecurityContract;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * Separate, bounded enrollment input. Production does not inherit this single-host qualification
 * provider.
 */
public record MountedNativeContract(
    int schemaVersion,
    String qualification,
    String cell,
    long storageEpoch,
    long routingEpoch,
    String environment,
    IdentitySecurityContract identity,
    NativeControlDatabaseEnrollment.Database cellDatabase,
    NativeControlDatabaseEnrollment.Database directoryDatabase,
    Tls tls,
    Sources sources,
    Proofs proofs,
    Map<String, String> jwtPublicKeys,
    List<CellTarget> cells,
    List<GatewayTarget> gateways,
    String directorySnapshot,
    String directorySnapshotSha256) {
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(8192)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  public record Tls(
      String ca,
      String certificate,
      String privateKey,
      String remotingKeyStore,
      String remotingTrustStore) {}

  public record Sources(String clockUrl, String revocationUrl, Map<String, String> publicKeys) {
    public Sources {
      publicKeys = keys(publicKeys, 256);
      NativeSourceHttp.validateEndpoint(URI.create(clockUrl));
      NativeSourceHttp.validateEndpoint(URI.create(revocationUrl));
    }
  }

  public record Proofs(String keyId, String privateKey, Map<String, String> publicKeys) {
    public Proofs {
      if (keyId == null || !keyId.matches("[A-Za-z0-9_-]{1,64}"))
        throw new IllegalArgumentException();
      publicKeys = keys(publicKeys, 256);
    }
  }

  public record CellTarget(
      String cell, String host, int port, String tlsAuthority, long storageEpoch) {
    public CellTarget {
      validCell(cell);
      validHost(host);
      validHost(tlsAuthority);
      validPort(port);
      if (storageEpoch < 1) throw new IllegalArgumentException();
    }
  }

  public record GatewayTarget(
      String cell, String gatewayId, String host, int port, String tlsAuthority) {
    public GatewayTarget {
      validCell(cell);
      validHost(host);
      validHost(tlsAuthority);
      validPort(port);
      if (gatewayId == null || !gatewayId.matches("[A-Za-z0-9_.-]{1,128}"))
        throw new IllegalArgumentException();
    }
  }

  public MountedNativeContract {
    if (schemaVersion != 1
        || !"LOCAL_TEST_ONLY".equals(qualification)
        || !"local-test-only".equals(environment)
        || storageEpoch < 1
        || routingEpoch < 1) throw new IllegalArgumentException();
    validCell(cell);
    Objects.requireNonNull(identity);
    Objects.requireNonNull(cellDatabase);
    Objects.requireNonNull(directoryDatabase);
    if (!cellDatabase.jdbcUrl().contains("sslmode=verify-full")
        || !directoryDatabase.jdbcUrl().contains("sslmode=verify-full"))
      throw new IllegalArgumentException();
    Objects.requireNonNull(tls);
    Objects.requireNonNull(sources);
    Objects.requireNonNull(proofs);
    jwtPublicKeys = keys(jwtPublicKeys, 64);
    cells = List.copyOf(cells);
    gateways = List.copyOf(gateways);
    var enrolledCells =
        cells.stream()
            .map(CellTarget::cell)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    if (cells.isEmpty()
        || cells.size() > 50
        || cells.stream().map(CellTarget::cell).distinct().count() != cells.size()
        || cells.stream().noneMatch(c -> c.cell().equals(cell) && c.storageEpoch() == storageEpoch)
        || gateways.isEmpty()
        || gateways.size() > 128
        || gateways.stream().map(g -> g.cell() + "/" + g.gatewayId()).distinct().count()
            != gateways.size()
        || gateways.stream().anyMatch(g -> !enrolledCells.contains(g.cell()))
        || !proofs.publicKeys().containsKey(cell + "/" + proofs.keyId())
        || directorySnapshotSha256 == null
        || !directorySnapshotSha256.matches("[a-f0-9]{64}")) throw new IllegalArgumentException();
  }

  public static MountedNativeContract load(Path file, String mode, String acknowledgement) {
    try {
      if (!"local-minikube".equals(mode)
          || !"LOCAL_TEST_ONLY".equals(acknowledgement)
          || file == null
          || !file.isAbsolute()
          || !Files.isRegularFile(file)) throw new IllegalArgumentException();
      byte[] bytes;
      try (var input = Files.newInputStream(file)) {
        bytes = input.readNBytes(65537);
        if (bytes.length > 65536 || bytes.length == 0) throw new IllegalArgumentException();
      }
      var decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      var contract =
          JSON.readValue(
              decoder.decode(ByteBuffer.wrap(bytes)).toString(), MountedNativeContract.class);
      var root = file.getParent().normalize();
      var paths =
          new ArrayList<>(
              List.of(
                  contract.tls().ca(),
                  contract.tls().certificate(),
                  contract.tls().privateKey(),
                  contract.tls().remotingKeyStore(),
                  contract.tls().remotingTrustStore(),
                  contract.proofs().privateKey(),
                  contract.directorySnapshot()));
      paths.addAll(contract.sources().publicKeys().values());
      paths.addAll(contract.proofs().publicKeys().values());
      paths.addAll(contract.jwtPublicKeys().values());
      for (var name : paths) {
        var path = Path.of(name);
        if (!path.isAbsolute() || !path.normalize().startsWith(root) || name.length() > 512)
          throw new IllegalArgumentException();
      }
      return contract;
    } catch (Exception invalid) {
      // Jackson diagnostics may include private input; retain neither source fragments nor causes.
      throw new IllegalArgumentException("Invalid mounted native enrollment");
    }
  }

  private static Map<String, String> keys(Map<String, String> values, int maximum) {
    if (values == null || values.isEmpty() || values.size() > maximum)
      throw new IllegalArgumentException();
    values.forEach(
        (key, value) -> {
          if (key == null || key.length() > 128 || value == null || value.length() > 512)
            throw new IllegalArgumentException();
        });
    return Map.copyOf(values);
  }

  private static void validCell(String cell) {
    if (cell == null || !cell.matches("c[0-9]{3}")) throw new IllegalArgumentException();
  }

  private static void validHost(String host) {
    if (host == null || host.length() > 253 || !host.matches("[A-Za-z0-9.-]+"))
      throw new IllegalArgumentException();
  }

  private static void validPort(int port) {
    if (port < 1 || port > 65535) throw new IllegalArgumentException();
  }

  @Override
  public String toString() {
    return "MountedNativeContract[LOCAL_TEST_ONLY,credentials=redacted]";
  }
}
