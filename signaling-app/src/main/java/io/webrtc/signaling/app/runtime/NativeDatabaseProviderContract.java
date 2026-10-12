package io.webrtc.signaling.app.runtime;

import java.net.URI;
import java.time.Duration;
import java.util.*;

/**
 * Required production provider inputs, never a fencing receipt or a backup/restore success.
 * Provider adapters supply actual authenticated evidence to native promotion independently.
 */
public record NativeDatabaseProviderContract(
    String owner,
    String clusterIdentity,
    String region,
    List<String> availabilityZones,
    Fencing fencing,
    Archive archive,
    Restore restore,
    Tuning tuning) {
  public NativeDatabaseProviderContract {
    label(owner);
    label(clusterIdentity);
    label(region);
    availabilityZones = List.copyOf(availabilityZones);
    if (availabilityZones.size() != 3 || new HashSet<>(availabilityZones).size() != 3)
      throw new IllegalArgumentException("Three explicit provider availability zones required");
    availabilityZones.forEach(NativeDatabaseProviderContract::label);
    Objects.requireNonNull(fencing);
    Objects.requireNonNull(archive);
    Objects.requireNonNull(restore);
    Objects.requireNonNull(tuning);
  }

  public record SecretReference(String secretName, String key) {
    public SecretReference {
      if (secretName == null
          || !secretName.matches("[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]|[a-z0-9]")
          || key == null
          || !key.matches("[A-Za-z0-9._-]{1,253}"))
        throw new IllegalArgumentException("Explicit Kubernetes Secret name/key required");
    }

    @Override
    public String toString() {
      return "SecretReference[redacted]";
    }
  }

  public record Fencing(
      URI physicalFenceEndpoint,
      URI witnessEndpoint,
      SecretReference physicalFenceCredential,
      SecretReference witnessCredential,
      String trustBundleSecret,
      String witnessQuorumTopology,
      Duration maximumEvidenceAge,
      String acknowledgedWalVerificationContract) {
    public Fencing {
      https(physicalFenceEndpoint);
      https(witnessEndpoint);
      Objects.requireNonNull(physicalFenceCredential);
      Objects.requireNonNull(witnessCredential);
      label(trustBundleSecret);
      label(witnessQuorumTopology);
      label(acknowledgedWalVerificationContract);
      if (maximumEvidenceAge == null
          || maximumEvidenceAge.isZero()
          || maximumEvidenceAge.isNegative()
          || maximumEvidenceAge.compareTo(Duration.ofSeconds(5)) > 0)
        throw new IllegalArgumentException("Fresh exact-writer fencing/witness evidence required");
    }
  }

  public enum ArchiveProvider {
    S3,
    AZURE_BLOB,
    GOOGLE_CLOUD_STORAGE
  }

  public record ArchiveCredentials(
      SecretReference accessKeyId,
      SecretReference secretAccessKey,
      SecretReference sessionToken,
      SecretReference connectionString,
      SecretReference applicationCredentials) {}

  public record Archive(
      ArchiveProvider provider,
      String pluginVersion,
      String pluginImageDigest,
      URI endpoint,
      String destination,
      String serverName,
      ArchiveCredentials credentials,
      String trustBundleSecret,
      Duration walArchiveTimeout,
      Duration retentionWindow,
      String encryptionKeyReference,
      String networkPolicyReference) {
    public Archive {
      Objects.requireNonNull(provider);
      https(endpoint);
      label(serverName);
      label(trustBundleSecret);
      label(encryptionKeyReference);
      label(networkPolicyReference);
      if (pluginVersion == null
          || !pluginVersion.matches("[0-9]+\\.[0-9]+\\.[0-9]+")
          || pluginImageDigest == null
          || !pluginImageDigest.matches("sha256:[a-f0-9]{64}"))
        throw new IllegalArgumentException("Pinned provider plugin version/image digest required");
      Objects.requireNonNull(credentials);
      switch (provider) {
        case S3 -> {
          Objects.requireNonNull(credentials.accessKeyId());
          Objects.requireNonNull(credentials.secretAccessKey());
          if (credentials.connectionString() != null
              || credentials.applicationCredentials() != null)
            throw new IllegalArgumentException("Archive credentials differ from S3 provider");
        }
        case AZURE_BLOB -> {
          Objects.requireNonNull(credentials.connectionString());
          if (credentials.accessKeyId() != null
              || credentials.secretAccessKey() != null
              || credentials.sessionToken() != null
              || credentials.applicationCredentials() != null)
            throw new IllegalArgumentException("Archive credentials differ from Azure provider");
        }
        case GOOGLE_CLOUD_STORAGE -> {
          Objects.requireNonNull(credentials.applicationCredentials());
          if (credentials.accessKeyId() != null
              || credentials.secretAccessKey() != null
              || credentials.sessionToken() != null
              || credentials.connectionString() != null)
            throw new IllegalArgumentException("Archive credentials differ from Google provider");
        }
      }
      if (destination == null
          || destination.isBlank()
          || destination.length() > 1024
          || destination.chars().anyMatch(Character::isISOControl))
        throw new IllegalArgumentException("Explicit archive destination required");
      var target = URI.create(destination);
      String requiredScheme =
          switch (provider) {
            case S3 -> "s3";
            case AZURE_BLOB -> "https";
            case GOOGLE_CLOUD_STORAGE -> "gs";
          };
      if (!requiredScheme.equals(target.getScheme())
          || target.getHost() == null
          || target.getUserInfo() != null
          || target.getQuery() != null
          || target.getFragment() != null)
        throw new IllegalArgumentException("Archive destination differs from provider");
      if (walArchiveTimeout == null
          || walArchiveTimeout.isZero()
          || walArchiveTimeout.isNegative()
          || walArchiveTimeout.compareTo(Duration.ofMinutes(5)) > 0
          || retentionWindow == null
          || retentionWindow.compareTo(Duration.ofHours(24)) < 0
          || retentionWindow.compareTo(Duration.ofDays(365)) > 0)
        throw new IllegalArgumentException("Finite explicit WAL/archive retention required");
    }
  }

  public record Restore(
      String runbookReference,
      String isolatedDestination,
      SecretReference credential,
      String acknowledgedWalManifestSource,
      String epochRotationProcedure,
      Duration recoveryTimeBudget) {
    public Restore {
      label(runbookReference);
      label(isolatedDestination);
      Objects.requireNonNull(credential);
      label(acknowledgedWalManifestSource);
      label(epochRotationProcedure);
      if (recoveryTimeBudget == null
          || recoveryTimeBudget.isZero()
          || recoveryTimeBudget.isNegative()
          || recoveryTimeBudget.compareTo(Duration.ofDays(1)) > 0)
        throw new IllegalArgumentException("Explicit isolated restore bound required");
    }
  }

  public record Tuning(
      int autovacuumWorkers,
      int maintenanceWorkMemMiB,
      int vacuumCostLimit,
      int vacuumCostDelayMillis,
      double vacuumScaleFactor,
      double analyzeScaleFactor,
      int freezeMaxAge,
      int multixactFreezeMaxAge,
      Duration autovacuumNaptime,
      String qualificationEvidence) {
    public Tuning {
      if (autovacuumWorkers < 1
          || autovacuumWorkers > 64
          || maintenanceWorkMemMiB < 16
          || maintenanceWorkMemMiB > 16384
          || vacuumCostLimit < 1
          || vacuumCostLimit > 100000
          || vacuumCostDelayMillis < 0
          || vacuumCostDelayMillis > 1000
          || !Double.isFinite(vacuumScaleFactor)
          || vacuumScaleFactor < 0
          || vacuumScaleFactor > 1
          || !Double.isFinite(analyzeScaleFactor)
          || analyzeScaleFactor < 0
          || analyzeScaleFactor > 1
          || freezeMaxAge < 100000
          || freezeMaxAge > 2000000000
          || multixactFreezeMaxAge < 100000
          || multixactFreezeMaxAge > 2000000000
          || autovacuumNaptime == null
          || autovacuumNaptime.compareTo(Duration.ofSeconds(1)) < 0
          || autovacuumNaptime.compareTo(Duration.ofMinutes(10)) > 0)
        throw new IllegalArgumentException(
            "Explicit PostgreSQL vacuum/analyze/freeze tuning required");
      label(qualificationEvidence);
      if ("LOCAL_TEST_ONLY".equals(qualificationEvidence)
          || "NOT_QUALIFIED".equals(qualificationEvidence))
        throw new IllegalArgumentException("Production provider tuning evidence required");
    }
  }

  private static void https(URI uri) {
    if (uri == null
        || !"https".equals(uri.getScheme())
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getQuery() != null
        || uri.getFragment() != null
        || uri.toString().length() > 2048)
      throw new IllegalArgumentException("Explicit provider HTTPS endpoint required");
  }

  private static void label(String value) {
    if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}"))
      throw new IllegalArgumentException("Explicit provider input reference required");
  }

  @Override
  public String toString() {
    return "NativeDatabaseProviderContract[credentials=redacted]";
  }
}
