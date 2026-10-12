package io.webrtc.signaling.app.runtime;

import java.util.Objects;

/** Render-only Barman ObjectStore/Cluster patch; credentials remain Secret name/key references. */
public final class NativeDatabaseProviderRenderer {
  private NativeDatabaseProviderRenderer() {}

  public static String render(
      NativeDatabaseProviderContract contract,
      String objectName,
      NativeDatabaseProviderContract.SecretReference endpointCa) {
    Objects.requireNonNull(contract);
    Objects.requireNonNull(endpointCa);
    if (objectName == null
        || !objectName.matches("[a-z0-9][a-z0-9-]{0,61}[a-z0-9]|[a-z0-9]")
        || !endpointCa.secretName().equals(contract.archive().trustBundleSecret()))
      throw new IllegalArgumentException(
          "Explicit archive object and matching CA reference required");
    var a = contract.archive();
    var t = contract.tuning();
    if (a.walArchiveTimeout().getNano() != 0
        || a.retentionWindow().getSeconds() % 86400 != 0
        || a.retentionWindow().getNano() != 0
        || t.autovacuumNaptime().getNano() != 0)
      throw new IllegalArgumentException("Provider duration cannot be represented exactly");
    StringBuilder out =
        new StringBuilder(
            "# RENDER ONLY: cluster patch and ObjectStore; no Secret values or deployment evidence.\n");
    out.append("# Pinned plugin: ")
        .append(a.pluginVersion())
        .append(" ")
        .append(a.pluginImageDigest())
        .append('\n');
    out.append("apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: ")
        .append(q(contract.clusterIdentity()))
        .append(
            "\nspec:\n  plugins:\n    - name: barman-cloud.cloudnative-pg.io\n      isWALArchiver: true\n      parameters:\n        barmanObjectName: ")
        .append(q(objectName))
        .append("\n        serverName: ")
        .append(q(a.serverName()))
        .append("\n  postgresql:\n    parameters:\n");
    parameter(out, "archive_timeout", a.walArchiveTimeout().toSeconds() + "s");
    parameter(out, "autovacuum_max_workers", t.autovacuumWorkers());
    parameter(out, "maintenance_work_mem", t.maintenanceWorkMemMiB() + "MB");
    parameter(out, "autovacuum_vacuum_cost_limit", t.vacuumCostLimit());
    parameter(out, "autovacuum_vacuum_cost_delay", t.vacuumCostDelayMillis() + "ms");
    parameter(out, "autovacuum_vacuum_scale_factor", t.vacuumScaleFactor());
    parameter(out, "autovacuum_analyze_scale_factor", t.analyzeScaleFactor());
    parameter(out, "autovacuum_freeze_max_age", t.freezeMaxAge());
    parameter(out, "autovacuum_multixact_freeze_max_age", t.multixactFreezeMaxAge());
    parameter(out, "autovacuum_naptime", t.autovacuumNaptime().toSeconds() + "s");
    out.append("---\napiVersion: barmancloud.cnpg.io/v1\nkind: ObjectStore\nmetadata:\n  name: ")
        .append(q(objectName))
        .append("\nspec:\n  configuration:\n    destinationPath: ")
        .append(q(a.destination()))
        .append("\n    endpointURL: ")
        .append(q(a.endpoint().toString()))
        .append("\n    endpointCA:\n");
    reference(out, "      ", endpointCa);
    switch (a.provider()) {
      case S3 -> {
        out.append("    s3Credentials:\n      accessKeyId:\n");
        reference(out, "        ", a.credentials().accessKeyId());
        out.append("      secretAccessKey:\n");
        reference(out, "        ", a.credentials().secretAccessKey());
        if (a.credentials().sessionToken() != null) {
          out.append("      sessionToken:\n");
          reference(out, "        ", a.credentials().sessionToken());
        }
      }
      case AZURE_BLOB -> {
        out.append("    azureCredentials:\n      connectionString:\n");
        reference(out, "        ", a.credentials().connectionString());
      }
      case GOOGLE_CLOUD_STORAGE -> {
        out.append("    googleCredentials:\n      applicationCredentials:\n");
        reference(out, "        ", a.credentials().applicationCredentials());
      }
    }
    out.append("  retentionPolicy: ").append(q(a.retentionWindow().toDays() + "d")).append('\n');
    return out.toString();
  }

  private static void reference(
      StringBuilder out, String indent, NativeDatabaseProviderContract.SecretReference reference) {
    out.append(indent)
        .append("name: ")
        .append(q(reference.secretName()))
        .append('\n')
        .append(indent)
        .append("key: ")
        .append(q(reference.key()))
        .append('\n');
  }

  private static void parameter(StringBuilder out, String key, Object value) {
    out.append("      ").append(key).append(": ").append(q(value.toString())).append('\n');
  }

  private static String q(String value) {
    if (value == null
        || value.contains("${")
        || value.contains("{{")
        || value.chars().anyMatch(Character::isISOControl))
      throw new IllegalArgumentException("Unresolved provider rendering input");
    return "'" + value.replace("'", "''") + "'";
  }
}
