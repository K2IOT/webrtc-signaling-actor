package io.webrtc.signaling.auth;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.EdECPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Cached, process-bound time attestation. Signature authenticates a required external monitor, not
 * clock quality.
 */
public final class ClockSafetyMonitor {
  private static final long MAX_AGE_NANOS = Duration.ofSeconds(5).toNanos();

  public record Report(
      String keyId,
      String cell,
      long storageEpoch,
      UUID podUid,
      UUID processBoot,
      long sequence,
      Instant observedAt,
      Instant validUntil,
      int pairUncertaintyMicros,
      int relativeRateErrorPpm,
      boolean continuous) {
    public Report {
      if (keyId == null
          || !keyId.matches("[A-Za-z0-9._:-]{1,128}")
          || cell == null
          || !cell.matches("[a-z][a-z0-9-]{0,23}")
          || storageEpoch < 1
          || sequence < 1
          || pairUncertaintyMicros < 0
          || relativeRateErrorPpm < 0)
        throw new IllegalArgumentException("Invalid clock report identity");
      Objects.requireNonNull(podUid);
      Objects.requireNonNull(processBoot);
      Objects.requireNonNull(observedAt);
      Objects.requireNonNull(validUntil);
      if (!validUntil.isAfter(observedAt)
          || Duration.between(observedAt, validUntil).compareTo(Duration.ofSeconds(5)) > 0)
        throw new IllegalArgumentException("Clock report exceeds five seconds");
    }
  }

  private record Accepted(Report report, Instant wall, long elapsed, long deadline) {}

  private record State(long highestSequence, long invalidation, Accepted accepted) {}

  private final String cell;
  private final long storageEpoch;
  private final UUID podUid, processBoot;
  private final Map<String, PublicKey> sources;
  private final Clock wall;
  private final LongSupplier elapsed;
  private final AtomicReference<State> state = new AtomicReference<>(new State(0, 0, null));

  public ClockSafetyMonitor(
      String cell,
      long storageEpoch,
      UUID podUid,
      UUID processBoot,
      Map<String, PublicKey> sources,
      Clock wall,
      LongSupplier elapsed) {
    if (cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || storageEpoch < 1
        || sources == null
        || sources.isEmpty()
        || sources.size() > 256
        || sources.entrySet().stream()
            .anyMatch(
                e ->
                    e.getKey() == null
                        || !e.getKey().matches("[A-Za-z0-9._:-]{1,128}")
                        || !(e.getValue() instanceof EdECPublicKey key)
                        || !key.getParams().getName().equals("Ed25519")))
      throw new IllegalArgumentException("Pinned Ed25519 clock source required");
    this.cell = cell;
    this.storageEpoch = storageEpoch;
    this.podUid = Objects.requireNonNull(podUid);
    this.processBoot = Objects.requireNonNull(processBoot);
    this.sources = Map.copyOf(sources);
    this.wall = Objects.requireNonNull(wall);
    this.elapsed = Objects.requireNonNull(elapsed);
  }

  /**
   * requestStarted is captured before source I/O; delayed replies cannot establish a fresh full
   * interval.
   */
  public boolean observe(Report report, String signed, long requestStarted) {
    var before = state.get();
    if (report == null
        || signed == null
        || signed.length() != 86
        || !signed.matches("[A-Za-z0-9_-]{86}")
        || !cell.equals(report.cell())
        || storageEpoch != report.storageEpoch()
        || !podUid.equals(report.podUid())
        || !processBoot.equals(report.processBoot())
        || report.sequence() <= before.highestSequence()) return false;
    var key = sources.get(report.keyId());
    if (key == null) return false;
    try {
      var signature = Signature.getInstance("Ed25519");
      signature.initVerify(key);
      signature.update(signingBytes(report));
      if (!signature.verify(Base64.getUrlDecoder().decode(signed))) return false;
      long now = elapsed.getAsLong();
      var currentWall = wall.instant();
      long transit = now - requestStarted;
      long uncertainty = TimeUnitConversion.microsToNanos(report.pairUncertaintyMicros());
      long age = Duration.between(report.observedAt(), currentWall).toNanos();
      if (transit < 0 || transit >= MAX_AGE_NANOS || age < -uncertainty || age >= MAX_AGE_NANOS)
        return false;
      if (!report.continuous()
          || report.pairUncertaintyMicros() > 250000
          || report.relativeRateErrorPpm() > 1000) {
        return publish(report.sequence(), before.invalidation(), null);
      }
      long duration = Duration.between(report.observedAt(), report.validUntil()).toNanos();
      long margin = uncertainty + duration * report.relativeRateErrorPpm() / 1_000_000;
      long remaining =
          Math.min(
              Duration.between(currentWall, report.validUntil()).toNanos() - margin,
              duration - transit - margin);
      if (remaining <= 0) return publish(report.sequence(), before.invalidation(), null);
      return publish(
          report.sequence(),
          before.invalidation(),
          new Accepted(report, currentWall, now, now + remaining));
    } catch (GeneralSecurityException | RuntimeException invalid) {
      return false;
    }
  }

  private boolean publish(long sequence, long invalidation, Accepted accepted) {
    while (true) {
      var current = state.get();
      if (current.invalidation() != invalidation || sequence <= current.highestSequence())
        return false;
      var next = new State(sequence, invalidation + (accepted == null ? 1 : 0), accepted);
      if (state.compareAndSet(current, next)) return accepted != null;
    }
  }

  /**
   * Pure bounded work; safe for actor admission and private health probes. Expiry/steps are
   * absorbing.
   */
  public boolean valid() {
    // Readers never wait for signature verification. A newer valid report can replace
    // this snapshot; a concurrent loss cannot authorize from the older snapshot.
    for (int attempt = 0; attempt < 2; attempt++) {
      var snapshot = state.get();
      var accepted = snapshot.accepted();
      if (accepted == null) return false;
      try {
        long now = elapsed.getAsLong();
        long age = now - accepted.elapsed();
        long wallAge = Duration.between(accepted.wall(), wall.instant()).toNanos();
        long difference = wallAge - age;
        long errorBound =
            TimeUnitConversion.microsToNanos(accepted.report().pairUncertaintyMicros())
                + Math.max(0, age) * accepted.report().relativeRateErrorPpm() / 1_000_000;
        if (age < 0
            || now - accepted.deadline() >= 0
            || difference > errorBound
            || difference < -errorBound) {
          if (state.compareAndSet(
              snapshot, new State(snapshot.highestSequence(), snapshot.invalidation() + 1, null)))
            return false;
          continue;
        }
        return state.get().invalidation() == snapshot.invalidation();
      } catch (RuntimeException invalid) {
        state.compareAndSet(
            snapshot, new State(snapshot.highestSequence(), snapshot.invalidation() + 1, null));
        return false;
      }
    }
    return false;
  }

  public void invalidate() {
    state.updateAndGet(
        current -> new State(current.highestSequence(), current.invalidation() + 1, null));
  }

  public static byte[] signingBytes(Report report) {
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      string(out, "signaling-clock-bound-v1");
      string(out, report.keyId());
      string(out, report.cell());
      out.writeLong(report.storageEpoch());
      uuid(out, report.podUid());
      uuid(out, report.processBoot());
      out.writeLong(report.sequence());
      instant(out, report.observedAt());
      instant(out, report.validUntil());
      out.writeInt(report.pairUncertaintyMicros());
      out.writeInt(report.relativeRateErrorPpm());
      out.writeBoolean(report.continuous());
      out.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void uuid(DataOutputStream out, UUID value) throws IOException {
    out.writeLong(value.getMostSignificantBits());
    out.writeLong(value.getLeastSignificantBits());
  }

  private static void instant(DataOutputStream out, Instant value) throws IOException {
    out.writeLong(value.getEpochSecond());
    out.writeInt(value.getNano());
  }

  private static final class TimeUnitConversion {
    static long microsToNanos(int value) {
      return (long) value * 1000;
    }
  }
}
