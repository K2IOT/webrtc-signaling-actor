package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

/** Bounded raw-abuse arrivals and counters, separate from authorized-operation histograms. */
final class SecurityWorkload {
  private final List<VirtualClient.ProbeKind> modes;
  private final int millionths;
  private final EnumMap<VirtualClient.ProbeKind, Counts> counts =
      new EnumMap<>(VirtualClient.ProbeKind.class);
  private String failure;
  private boolean generatorLimited;

  private static final class Counts {
    long attempted,
        pendingLogical,
        logicalRejected,
        pendingPhysical,
        physicalCompleted,
        verifiedRejected,
        unknown,
        cleanupUnknown,
        readPauseVerified;
    final Map<String, Long> outcomes = new TreeMap<>();

    Map<String, Object> snapshot() {
      return Map.of(
          "attempted",
          attempted,
          "pendingLogical",
          pendingLogical,
          "logicalRejected",
          logicalRejected,
          "pendingPhysical",
          pendingPhysical,
          "physicalCompleted",
          physicalCompleted,
          "verifiedRejected",
          verifiedRejected,
          "unknown",
          unknown,
          "cleanupUnknown",
          cleanupUnknown);
    }
  }

  private SecurityWorkload(List<VirtualClient.ProbeKind> modes, int millionths) {
    this.modes = List.copyOf(modes);
    this.millionths = millionths;
    for (var mode : modes) counts.put(mode, new Counts());
  }

  static SecurityWorkload parse(JsonNode scenario) {
    var modes = new ArrayList<VirtualClient.ProbeKind>();
    var requested = scenario.path("abuse");
    if (scenario.has("abuse")) {
      if (!requested.isArray() || requested.size() > 6)
        throw new IllegalArgumentException("Invalid abuse profile");
      for (var item : requested) {
        if (!item.isTextual()) throw new IllegalArgumentException("Invalid abuse mode");
        var mode =
            switch (item.textValue()) {
              case "malformed" -> VirtualClient.ProbeKind.MALFORMED;
              case "oversized" -> VirtualClient.ProbeKind.OVERSIZED;
              case "staleGeneration" -> VirtualClient.ProbeKind.SECURITY_CLOSURE;
              case "slowConsumer" -> VirtualClient.ProbeKind.SLOW_CONSUMER;
              default -> throw new IllegalArgumentException("ABUSE_PROFILE_NOT_IMPLEMENTED");
            };
        if (modes.contains(mode)) throw new IllegalArgumentException("Duplicate abuse mode");
        modes.add(mode);
      }
    }
    int parts = 0;
    if (scenario.has("abuseFraction")) {
      var fraction = scenario.path("abuseFraction");
      if (!fraction.isNumber()) throw new IllegalArgumentException("Invalid abuse fraction");
      try {
        parts = fraction.decimalValue().multiply(BigDecimal.valueOf(1000000)).intValueExact();
      } catch (ArithmeticException invalid) {
        throw new IllegalArgumentException("Invalid abuse fraction");
      }
    }
    if (parts < 0
        || parts > 1000000
        || modes.isEmpty() && parts != 0
        || !modes.isEmpty() && parts == 0)
      throw new IllegalArgumentException("Invalid abuse fraction");
    return new SecurityWorkload(modes, parts);
  }

  boolean enabled() {
    return !modes.isEmpty();
  }

  boolean staleEnabled() {
    return modes.contains(VirtualClient.ProbeKind.SECURITY_CLOSURE);
  }

  Optional<VirtualClient.ProbeKind> kind(long seed, long ordinal) {
    if (ordinal < 0) throw new IllegalArgumentException("Original arrival ordinal required");
    if (!enabled()) return Optional.empty();
    var hash = DistributedLoadGenerator.operation(seed, 0, "SECURITY", ordinal);
    if (Math.floorMod(hash.getLeastSignificantBits(), 1000000) >= millionths)
      return Optional.empty();
    return Optional.of(modes.get((int) Math.floorMod(hash.getMostSignificantBits(), modes.size())));
  }

  synchronized String failure() {
    return failure;
  }

  synchronized boolean generatorLimited() {
    return generatorLimited;
  }

  synchronized Map<String, Object> snapshot() {
    var total = new Counts();
    var byMode = new LinkedHashMap<String, Object>();
    counts.forEach(
        (mode, c) -> {
          var details = new LinkedHashMap<String, Object>(c.snapshot());
          details.put("outcomes", Map.copyOf(c.outcomes));
          if (mode == VirtualClient.ProbeKind.SLOW_CONSUMER)
            details.put("readPauseVerified", c.readPauseVerified);
          byMode.put(
              mode == VirtualClient.ProbeKind.SECURITY_CLOSURE ? "STALE_GENERATION" : mode.name(),
              details);
          total.attempted += c.attempted;
          total.pendingLogical += c.pendingLogical;
          total.logicalRejected += c.logicalRejected;
          total.pendingPhysical += c.pendingPhysical;
          total.physicalCompleted += c.physicalCompleted;
          total.verifiedRejected += c.verifiedRejected;
          total.unknown += c.unknown;
          total.cleanupUnknown += c.cleanupUnknown;
        });
    var result = new LinkedHashMap<String, Object>(total.snapshot());
    result.put("scope", "offeredSetupFrameArrivals");
    result.put("fractionMillionths", millionths);
    result.put("modes", byMode);
    return Collections.unmodifiableMap(result);
  }

  synchronized void unavailable(VirtualClient.ProbeKind kind) {
    var c = Objects.requireNonNull(counts.get(kind));
    c.attempted++;
    c.unknown++;
    c.outcomes.merge("ADMISSION_REJECTED", 1L, Long::sum);
    generatorLimited = true;
    failure = "SECURITY_GENERATOR_ADMISSION";
  }

  VirtualClient.ProbeOperation start(
      VirtualClient client, VirtualClient.ProbeKind kind, long intended) {
    if (kind == VirtualClient.ProbeKind.SECURITY_CLOSURE)
      throw new IllegalArgumentException("Native replacement proof required");
    final Counts c;
    long generation = client.generation();
    synchronized (this) {
      c = Objects.requireNonNull(counts.get(kind));
      c.attempted++;
      c.pendingLogical++;
      c.pendingPhysical++;
    }
    final VirtualClient.ProbeOperation original;
    try {
      original = client.probe(kind, intended);
    } catch (RuntimeException unknown) {
      synchronized (this) {
        c.pendingLogical--;
        c.unknown++;
        c.cleanupUnknown++;
        failure = "SECURITY_CLEANUP_UNKNOWN";
      }
      // A throwing factory provides no original physical receipt. Keep its admission UNKNOWN.
      return new VirtualClient.ProbeOperation(
          CompletableFuture.failedFuture(new IllegalStateException("Security start unknown")),
          new CompletableFuture<Void>(),
          CompletableFuture.completedFuture(false));
    }
    var observed =
        original
            .observed()
            .whenComplete(
                (receipt, error) -> {
                  synchronized (this) {
                    c.pendingLogical--;
                    if (error == null) c.outcomes.merge(receipt.outcome().name(), 1L, Long::sum);
                    if (error == null
                        && kind == VirtualClient.ProbeKind.SLOW_CONSUMER
                        && readPauseObserved(intended, receipt)) c.readPauseVerified++;
                    if (error == null && expected(kind, generation, intended, receipt))
                      c.logicalRejected++;
                    else {
                      c.unknown++;
                      boolean admission =
                          error == null
                              && (receipt.outcome() == VirtualClient.ProbeOutcome.ADMISSION_REJECTED
                                  || receipt.outcome()
                                      == VirtualClient.ProbeOutcome.CREDIT_REJECTED);
                      generatorLimited |= admission;
                      failure =
                          admission
                              ? "SECURITY_GENERATOR_ADMISSION"
                              : "SECURITY_REJECTION_NOT_OBSERVED";
                    }
                  }
                });
    // Publish physical success after both original receipts and their accounting callbacks settle.
    var physical =
        observed
            .handle((receipt, error) -> receipt)
            .thenCombine(
                original.physicalCompletion(),
                (receipt, v) -> {
                  synchronized (this) {
                    c.pendingPhysical--;
                    c.physicalCompleted++;
                    if (expected(kind, generation, intended, receipt)) c.verifiedRejected++;
                  }
                  return (Void) null;
                })
            .whenComplete(
                (v, error) -> {
                  if (error != null)
                    synchronized (this) {
                      c.cleanupUnknown++;
                      failure = "SECURITY_CLEANUP_UNKNOWN";
                    }
                });
    return new VirtualClient.ProbeOperation(observed, physical, original.admission());
  }

  StaleGenerationProbe.Operation startStale(
      VirtualClient original, VirtualClient newer, long intended) {
    final Counts c;
    long generation = original.generation();
    synchronized (this) {
      c = Objects.requireNonNull(counts.get(VirtualClient.ProbeKind.SECURITY_CLOSURE));
      c.attempted++;
      c.pendingLogical++;
      c.pendingPhysical++;
    }
    final StaleGenerationProbe.Operation operation;
    try {
      operation = StaleGenerationProbe.run(original, newer, intended);
    } catch (RuntimeException unknown) {
      synchronized (this) {
        c.pendingLogical--;
        c.unknown++;
        c.cleanupUnknown++;
        failure = "SECURITY_CLEANUP_UNKNOWN";
      }
      return new StaleGenerationProbe.Operation(
          CompletableFuture.failedFuture(new IllegalStateException("Security start unknown")),
          new CompletableFuture<Void>());
    }
    var observed =
        operation
            .observed()
            .whenComplete(
                (receipt, error) -> {
                  synchronized (this) {
                    c.pendingLogical--;
                    if (error == null) c.outcomes.merge(receipt.outcome().name(), 1L, Long::sum);
                    if (error == null && expectedStale(generation, intended, receipt))
                      c.logicalRejected++;
                    else {
                      c.unknown++;
                      failure = "SECURITY_REJECTION_NOT_OBSERVED";
                    }
                  }
                });
    var physical =
        observed
            .handle((receipt, error) -> receipt)
            .thenCombine(
                operation.physicalCompletion(),
                (receipt, v) -> {
                  synchronized (this) {
                    c.pendingPhysical--;
                    c.physicalCompleted++;
                    if (expectedStale(generation, intended, receipt)) c.verifiedRejected++;
                  }
                  return (Void) null;
                })
            .whenComplete(
                (v, error) -> {
                  if (error != null)
                    synchronized (this) {
                      c.cleanupUnknown++;
                      failure = "SECURITY_CLEANUP_UNKNOWN";
                    }
                });
    return new StaleGenerationProbe.Operation(observed, physical);
  }

  private static boolean expectedStale(
      long generation, long intended, StaleGenerationProbe.Receipt receipt) {
    var closure = receipt == null ? null : receipt.closure();
    long elapsed = closure == null ? -1 : closure.finishedNanos() - intended,
        authElapsed = receipt == null ? -1 : receipt.replacementFinishedNanos() - intended;
    return receipt != null
        && receipt.outcome() == StaleGenerationProbe.Outcome.REPLACED
        && receipt.originalGeneration() > 0
        && receipt.replacementGeneration() > receipt.originalGeneration()
        && closure != null
        && closure.generation() == generation
        && closure.intendedNanos() == intended
        && elapsed >= 0
        && elapsed < TimeUnit.SECONDS.toNanos(5)
        && authElapsed >= 0
        && authElapsed < TimeUnit.SECONDS.toNanos(5);
  }

  private static boolean readPauseObserved(long intended, VirtualClient.ProbeReceipt receipt) {
    var pause = receipt == null ? null : receipt.readPause();
    if (pause == null) return false;
    long started = pause.startedNanos() - intended, resumed = pause.resumedNanos() - intended;
    return started >= 0
        && started < TimeUnit.SECONDS.toNanos(2)
        && resumed >= TimeUnit.SECONDS.toNanos(10)
        && resumed < TimeUnit.SECONDS.toNanos(12)
        && pause.resumedNanos() >= pause.startedNanos()
        && pause.resumedNanos() <= receipt.finishedNanos();
  }

  private static boolean expected(
      VirtualClient.ProbeKind kind,
      long generation,
      long intended,
      VirtualClient.ProbeReceipt receipt) {
    long elapsed = receipt == null ? -1 : receipt.finishedNanos() - intended;
    if (kind == VirtualClient.ProbeKind.SLOW_CONSUMER)
      return receipt != null
          && receipt.kind() == kind
          && receipt.generation() == generation
          && receipt.intendedNanos() == intended
          && elapsed >= 0
          && elapsed < TimeUnit.SECONDS.toNanos(12)
          && readPauseObserved(intended, receipt)
          && receipt.outcome() == VirtualClient.ProbeOutcome.BACKPRESSURE_REJECTED
          && receipt.closeCode() == 1013
          && receipt.closeReason() == VirtualClient.CloseReason.RESYNC_REQUIRED;
    return receipt != null
        && receipt.kind() == kind
        && receipt.generation() == generation
        && receipt.intendedNanos() == intended
        && elapsed >= 0
        && elapsed < TimeUnit.SECONDS.toNanos(2)
        && receipt.outcome() == VirtualClient.ProbeOutcome.PROTOCOL_REJECTED
        && receipt.closeCode() == (kind == VirtualClient.ProbeKind.MALFORMED ? 1002 : 1009);
  }
}
