package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.*;
import java.sql.Connection;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Explicit operator fence, original-owner drains and actual writer reads. Existing durable rows have
 * no executable provenance: their target compatibility remains UNKNOWN, even when counts are small.
 */
public final class NativeRollbackCollector {
  public record Policy(String sourceExecutable, String targetExecutable, Set<String> processIds,
      Set<String> ownerIds, Map<String, Long> writerEpochs, Map<String, Integer> retainedMigrations) {
    public Policy {
      processIds = Set.copyOf(processIds); ownerIds = Set.copyOf(ownerIds);
      writerEpochs = Map.copyOf(writerEpochs); retainedMigrations = Map.copyOf(retainedMigrations);
      if (sourceExecutable == null || !sourceExecutable.matches("[a-f0-9]{64}")
          || targetExecutable == null || !targetExecutable.matches("[a-f0-9]{64}")
          || processIds.isEmpty() || processIds.size() > 478 || ownerIds.isEmpty() || ownerIds.size() > 4096
          || writerEpochs.isEmpty() || writerEpochs.size() > 50 || retainedMigrations.isEmpty()
          || retainedMigrations.size() > 64 || writerEpochs.values().stream().anyMatch(e -> e < 1))
        throw new IllegalArgumentException("Complete bounded rollback policy required");
    }
  }
  /** Operator roster must include every actor, gateway, control, maintenance and external writer owner. */
  public record Enrollment(Policy policy, NativeCompatibilityEnrollment.BuildIdentity installedBuild,
      Map<String, NativeCompatibility> processGates, Map<String, Supplier<CompletionStage<Void>>> permanentOwnerDrains,
      Map<String, SqlTransactions> observationWriters) {
    public Enrollment {
      Objects.requireNonNull(policy); Objects.requireNonNull(installedBuild);
      processGates = Map.copyOf(processGates); permanentOwnerDrains = Map.copyOf(permanentOwnerDrains);
      observationWriters = Map.copyOf(observationWriters);
      if (!policy.sourceExecutable().equals(installedBuild.executableId())
          || !policy.processIds().equals(processGates.keySet())
          || !policy.ownerIds().equals(permanentOwnerDrains.keySet())
          || !policy.writerEpochs().keySet().equals(observationWriters.keySet()))
        throw new IllegalArgumentException("Rollback roster incomplete");
    }
  }
  private record WriterFact(boolean exactSchema, boolean emptyKnownWork) {}
  private static final Set<String> INFRASTRUCTURE = Set.of("flyway_schema_history", "cell_authority",
      "bucket_authority", "group_owner", "security_epoch", "security_progress", "retired_signing_key");
  private static final Set<String> DURABLE = Set.of("user_guard", "gateway_lease", "session_registry",
      "user_reservation", "home_participation", "call_state", "command_result", "control_outbox",
      "call_workflow_step", "privacy_deletion_request", "recovery_epoch_journal", "promotion_epoch_journal",
      "bucket_transfer", "bucket_transfer_item");
  private final Enrollment inputs;
  private Map<String, UUID> fence;
  private CompletableFuture<Void> originalOwners;
  private volatile NativeCompatibilityEnrollment.RollbackObservation facts;

  public NativeRollbackCollector(Enrollment inputs) { this.inputs = Objects.requireNonNull(inputs); }

  /** Trusted native operator action only; no RPC or public administration endpoint invokes it. */
  public synchronized Map<String, UUID> beginFence() {
    if (fence != null) return fence;
    var generations = new HashMap<String, UUID>();
    inputs.processGates().forEach((id, gate) -> {
      gate.disableFeatureAdmission(); generations.put(id, gate.admissionFence());
    });
    fence = Map.copyOf(generations);
    var receipts = new ArrayList<CompletableFuture<Void>>();
    for (var drain : inputs.permanentOwnerDrains().values()) {
      try { receipts.add(Objects.requireNonNull(drain.get()).toCompletableFuture()); }
      catch (RuntimeException unknown) { receipts.add(new CompletableFuture<>()); }
    }
    originalOwners = CompletableFuture.allOf(receipts.toArray(CompletableFuture[]::new));
    return fence;
  }

  private synchronized boolean fencedAndSettled() {
    return fence != null && originalOwners != null && originalOwners.isDone()
        && !originalOwners.isCompletedExceptionally() && !originalOwners.isCancelled()
        && inputs.processGates().entrySet().stream().allMatch(e -> !e.getValue().admissionOpen()
            && Objects.equals(fence.get(e.getKey()), e.getValue().admissionFence()));
  }
  public NativeCompatibilityEnrollment.RollbackObservation observation() {
    var value = facts;
    return fencedAndSettled() && value != null && value.validUntil().isAfter(Instant.now()) ? value : null;
  }
  public NativeWorkerScheduler.Job job() {
    return new NativeWorkerScheduler.Job("compatibility_rollback", NativeWorkerScheduler.Priority.SAFETY,
        Duration.ofSeconds(1), this::collect);
  }
  private RpcOperation<?> collect(Duration budget) {
    if (!fencedAndSettled()) { facts = null; return completed(); }
    long end = System.nanoTime() + budget.toNanos();
    var logical = new ArrayList<CompletableFuture<WriterFact>>();
    var physical = new ArrayList<CompletableFuture<?>>();
    for (var entry : inputs.observationWriters().entrySet()) {
      try {
        long remaining = end - System.nanoTime();
        if (remaining <= 0) { facts = null; return retainedFailure(physical); }
        var operation = entry.getValue().submitTracked(DbClass.RECOVERY, Duration.ofNanos(remaining),
            connection -> inspectWriter(connection, entry.getKey()));
        logical.add(operation.logical().toCompletableFuture());
        physical.add(operation.physicalCompletion().toCompletableFuture());
      } catch (RuntimeException unknown) {
        facts = null; physical.add(new CompletableFuture<>()); return retainedFailure(physical);
      }
    }
    var result = CompletableFuture.allOf(logical.toArray(CompletableFuture[]::new)).thenRun(() -> {
      if (!fencedAndSettled() || System.nanoTime() >= end) { facts = null; return; }
      boolean empty = logical.stream().allMatch(f -> f.join().emptyKnownWork());
      boolean schema = logical.stream().allMatch(f -> f.join().exactSchema());
      facts = new NativeCompatibilityEnrollment.RollbackObservation(Instant.now().plusSeconds(5),
          empty ? OptionalLong.of(0) : OptionalLong.empty(), Optional.of(true), Optional.of(schema));
    });
    result.whenComplete((v, e) -> { if (e != null) facts = null; });
    return new RpcOperation<>(result, CompletableFuture.allOf(physical.toArray(CompletableFuture[]::new)));
  }
  private WriterFact inspectWriter(Connection connection, String cell) throws Exception {
    try (var statement = connection.prepareStatement(
        "SELECT cell_id,storage_epoch,pg_is_in_recovery(),pg_try_advisory_xact_lock(100,1) FROM cell_authority WHERE singleton_id=1")) {
      statement.setQueryTimeout(2);
      try (var rows = statement.executeQuery()) {
        if (!rows.next() || !cell.equals(rows.getString(1)) || rows.getLong(2) != inputs.policy().writerEpochs().get(cell)
            || rows.getBoolean(3) || !rows.getBoolean(4) || rows.next()) throw new IllegalStateException("Writer authority unknown");
      }
    }
    var applied = new HashMap<String, Integer>();
    try (var statement = connection.prepareStatement("SELECT version,checksum,success FROM flyway_schema_history ORDER BY installed_rank LIMIT 65")) {
      statement.setQueryTimeout(2);
      try (var rows = statement.executeQuery()) {
        while (rows.next()) {
          String version = rows.getString(1); int checksum = rows.getInt(2);
          if (version == null || rows.wasNull() || !rows.getBoolean(3) || applied.put(version, checksum) != null || applied.size() > 64)
            throw new IllegalStateException("Schema identity unknown");
        }
      }
    }
    var actualTables = new HashSet<String>();
    try (var statement = connection.prepareStatement("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid) LIMIT 65")) {
      statement.setQueryTimeout(2);
      try (var rows = statement.executeQuery()) { while (rows.next()) actualTables.add(rows.getString(1)); }
    }
    var expected = new HashSet<>(INFRASTRUCTURE); expected.addAll(DURABLE);
    boolean empty = actualTables.equals(expected);
    if (empty) for (String table : DURABLE) {
      // Table names come exclusively from the fixed source catalog, never policy or request text.
      try (var statement = connection.prepareStatement("SELECT EXISTS (SELECT 1 FROM " + table + " LIMIT 1)")) {
        statement.setQueryTimeout(2);
        try (var rows = statement.executeQuery()) { if (!rows.next() || rows.getBoolean(1)) { empty = false; break; } }
      }
    }
    return new WriterFact(applied.equals(inputs.policy().retainedMigrations()), empty);
  }
  private static RpcOperation<?> completed() {
    return new RpcOperation<>(CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
  }
  private static RpcOperation<?> retainedFailure(List<CompletableFuture<?>> owners) {
    return new RpcOperation<>(CompletableFuture.failedFuture(new IllegalStateException("Rollback writer facts unknown")),
        CompletableFuture.allOf(owners.toArray(CompletableFuture[]::new)));
  }
}
