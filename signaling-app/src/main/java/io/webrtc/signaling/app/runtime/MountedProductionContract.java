package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/**
 * Strict production-only input. Loading validates mounted references; it never starts a provider.
 */
public record MountedProductionContract(
    int schemaVersion,
    String environment,
    Set<String> profiles,
    String cell,
    long storageEpoch,
    MountedReference operatorAttestation,
    MountedReference identityContract,
    MountedReference workloadTrust,
    MountedReference cellDatabase,
    MountedReference directoryDatabase,
    MountedReference clockSource,
    MountedReference revocationSource,
    MountedReference fenceProviderImplementation,
    MountedReference witnessProviderImplementation,
    MountedReference acknowledgedWalManifest,
    NativeDatabaseProviderContract databaseProvider,
    NativeWalEnrollment wal) {
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(12)
                          .maxStringLength(8192)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  public record MountedReference(String path, String sha256) {
    public MountedReference {
      if (path == null
          || !Path.of(path).isAbsolute()
          || !Path.of(path).normalize().toString().equals(path)
          || path.length() > 1024
          || sha256 == null
          || !sha256.matches("[a-f0-9]{64}"))
        throw new IllegalArgumentException("Canonical mounted reference and digest required");
    }
  }

  public MountedProductionContract {
    profiles = Set.copyOf(profiles);
    if (schemaVersion != 1
        || !"production".equals(environment)
        || profiles.isEmpty()
        || !Set.of("actor", "gateway", "control").containsAll(profiles)
        || profiles.size() != 1
        || cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || storageEpoch < 1)
      throw new IllegalArgumentException("Explicit single-plane production contract required");
    Objects.requireNonNull(operatorAttestation);
    Objects.requireNonNull(identityContract);
    Objects.requireNonNull(workloadTrust);
    Objects.requireNonNull(cellDatabase);
    Objects.requireNonNull(directoryDatabase);
    Objects.requireNonNull(clockSource);
    Objects.requireNonNull(revocationSource);
    Objects.requireNonNull(fenceProviderImplementation);
    Objects.requireNonNull(witnessProviderImplementation);
    Objects.requireNonNull(acknowledgedWalManifest);
    Objects.requireNonNull(databaseProvider);
    Objects.requireNonNull(wal);
    if (wal.localSingleWriter() || !cell.equals(wal.cell()) || storageEpoch != wal.storageEpoch())
      throw new IllegalArgumentException("Production WAL enrollment differs from authority");
  }

  public static MountedProductionContract load(
      Path file, Path approvedMountRoot, Set<String> activeProfiles) throws Exception {
    Path root = approvedMountRoot.toRealPath();
    byte[] bytes = bounded(file, root, 131072);
    String text =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString();
    if (text.contains("${")
        || text.contains("{{")
        || text.contains("LOCAL_TEST_ONLY")
        || text.contains("NOT_QUALIFIED"))
      throw new IllegalArgumentException("Unresolved or local production contract input");
    var contract = JSON.readValue(text, MountedProductionContract.class);
    if (!activeProfiles.equals(contract.profiles()))
      throw new IllegalArgumentException("Production profile mismatch");
    for (var ref :
        List.of(
            contract.operatorAttestation(),
            contract.identityContract(),
            contract.workloadTrust(),
            contract.cellDatabase(),
            contract.directoryDatabase(),
            contract.clockSource(),
            contract.revocationSource(),
            contract.fenceProviderImplementation(),
            contract.witnessProviderImplementation(),
            contract.acknowledgedWalManifest())) {
      byte[] content = bounded(Path.of(ref.path()), root, 1048576);
      if (!MessageDigest.isEqual(
          HexFormat.of().parseHex(ref.sha256()),
          MessageDigest.getInstance("SHA-256").digest(content)))
        throw new IllegalArgumentException("Mounted production reference digest mismatch");
    }
    return contract;
  }

  private static byte[] bounded(Path path, Path root, int maximum) throws Exception {
    Path actual = path.toRealPath();
    if (!actual.startsWith(root)
        || !Files.isRegularFile(actual)
        || Files.size(actual) < 1
        || Files.size(actual) > maximum)
      throw new IllegalArgumentException("Production input outside approved mounted boundary");
    try (var input = Files.newInputStream(actual)) {
      byte[] bytes = input.readNBytes(maximum + 1);
      if (bytes.length == 0 || bytes.length > maximum)
        throw new IllegalArgumentException("Production input outside bound");
      return bytes;
    }
  }

  @Override
  public String toString() {
    return "MountedProductionContract[references=redacted]";
  }
}
