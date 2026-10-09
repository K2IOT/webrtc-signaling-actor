package io.webrtc.signaling.gateway;

import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Traffic-driven R1 reuse. Original physical receipts own pending credit through UNKNOWN. */
public final class NativeRelaySessionProofCache {
  private static final Duration MARGIN = Duration.ofMillis(255);
  // Conservatively accounts the bounded ASCII proof, full identity key and completion state.
  public static final int ENTRY_BYTES = 16384;

  private record Key(
      AuthenticatedSession sender,
      CallId call,
      long round,
      long ice,
      ProofBindings.TrustedHome home) {}

  private static final class Entry {
    final CompletableFuture<String> logical = new CompletableFuture<>();
    final CompletableFuture<Void> physical = new CompletableFuture<>();
    final long started;
    final Instant wallStart;
    final long epoch;
    boolean cleaned, finished;
    long expiresNanos;
    Instant checkedAt, until;

    Entry(long started, Instant wallStart, long epoch) {
      this.started = started;
      this.wallStart = wallStart;
      this.epoch = epoch;
    }

    RpcOperation<String> operation() {
      return new RpcOperation<>(logical, physical);
    }
  }

  private final int capacity, maxPending;
  private final Clock clock;
  private final LongSupplier ticks;
  private final BooleanSupplier healthy;
  private final RelaySessionAuthorizationProof proofs;
  private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>(16, .75f, true);
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean closed;
  private long epoch;

  public NativeRelaySessionProofCache(
      int capacity,
      int maxPending,
      Clock clock,
      LongSupplier ticks,
      BooleanSupplier healthy,
      RelaySessionAuthorizationProof proofs) {
    if (capacity < 1
        || capacity > 4096
        || maxPending < 1
        || maxPending > 64
        || maxPending > capacity) throw new IllegalArgumentException("Invalid bounded proof cache");
    this.capacity = capacity;
    this.maxPending = maxPending;
    this.clock = Objects.requireNonNull(clock);
    this.ticks = Objects.requireNonNull(ticks);
    this.healthy = Objects.requireNonNull(healthy);
    this.proofs = Objects.requireNonNull(proofs);
  }

  public synchronized RpcOperation<String> get(
      CallCommand command,
      ProofBindings.TrustedHome home,
      Duration budget,
      Supplier<RpcOperation<String>> loader) {
    if (!RelaySessionAuthorizationProof.supports(command)
        || home == null
        || budget == null
        || budget.isNegative()
        || budget.isZero()
        || budget.compareTo(Duration.ofSeconds(2)) > 0)
      return denied("Invalid relay proof request");
    if (closed) return denied("Relay proof cache draining");
    if (!healthy.getAsBoolean()) {
      invalidate();
      return denied("Relay proof safety unavailable");
    }
    var key =
        new Key(
            command.sender(),
            command.callId(),
            command.negotiationId().value(),
            command.iceGeneration().value(),
            home);
    var hit = entries.get(key);
    if (hit != null) {
      if (!hit.finished) {
        if (hit.logical.isDone() && !hit.logical.isCompletedExceptionally() && !live(hit))
          return new RpcOperation<>(
              CompletableFuture.failedFuture(
                  new IllegalStateException("Expired relay proof awaiting cleanup")),
              hit.physical);
        return view(hit, budget);
      }
      if (live(hit)) return view(hit, budget);
      entries.remove(key);
    }
    entries.entrySet().removeIf(e -> e.getValue().finished && !live(e.getValue()));
    if (pending() >= maxPending) return denied("Relay proof refresh overloaded");
    if (entries.size() >= capacity) {
      var eviction = entries.entrySet().stream().filter(e -> e.getValue().finished).findFirst();
      if (eviction.isEmpty()) return denied("Relay proof cache full");
      entries.remove(eviction.get().getKey());
    }
    var entry = new Entry(ticks.getAsLong(), clock.instant(), epoch);
    entries.put(key, entry);
    // Retain metadata only; source RPC owns its token/body buffers under its independent admission.
    var scope =
        new CallCommand(
            command.type(),
            command.sender(),
            command.requestId(),
            command.callId(),
            command.scope(),
            null,
            command.negotiationId(),
            command.iceGeneration(),
            "{}",
            command.intentHash());
    entry.logical.whenComplete(
        (v, e) -> {
          synchronized (this) {
            finish(key, entry);
          }
        });
    entry.logical.orTimeout(budget.toNanos(), TimeUnit.NANOSECONDS);
    final RpcOperation<String> original;
    try {
      original = Objects.requireNonNull(loader.get());
    } catch (Throwable unknown) {
      entry.logical.completeExceptionally(unknown);
      return view(entry, budget);
    }
    original
        .logical()
        .whenComplete(
            (signed, error) -> {
              synchronized (this) {
                if (error != null) {
                  entry.logical.completeExceptionally(error);
                  return;
                }
                try {
                  if (entry.logical.isDone()
                      || closed
                      || entry.epoch != epoch
                      || !healthy.getAsBoolean()
                      || !proofs.verify(signed, scope, home, clock.instant()))
                    throw new IllegalStateException("Stale relay session proof");
                  var view =
                      proofs
                          .decode(signed, home.cell(), clock.instant())
                          .orElseThrow()
                          .nativeView();
                  entry.checkedAt = view.checkedAt();
                  entry.until = view.proofUntil().minus(MARGIN);
                  entry.expiresNanos =
                      entry.started
                          + Math.min(
                              Duration.ofSeconds(5).minus(MARGIN).toNanos(),
                              Duration.between(entry.wallStart, entry.until).toNanos());
                  if (!live(entry)) throw new IllegalStateException("Expired original relay proof");
                  entry.logical.complete(signed);
                } catch (RuntimeException denied) {
                  entry.logical.completeExceptionally(denied);
                }
              }
            });
    original
        .physicalCompletion()
        .whenComplete(
            (v, error) -> {
              if (error == null)
                synchronized (this) {
                  entry.cleaned = true;
                  finish(key, entry);
                  entry.physical.complete(null);
                  if (closed && pending() == 0) drained.complete(null);
                }
            });
    return view(entry, budget);
  }

  private static RpcOperation<String> view(Entry entry, Duration budget) {
    var consumer =
        entry.logical.thenApply(value -> value).orTimeout(budget.toNanos(), TimeUnit.NANOSECONDS);
    return new RpcOperation<>(consumer, entry.physical);
  }

  private boolean live(Entry entry) {
    var now = clock.instant();
    return entry.epoch == epoch
        && entry.until != null
        && ticks.getAsLong() - entry.expiresNanos < 0
        && !entry.checkedAt.isAfter(now.plusMillis(250))
        && now.isBefore(entry.until);
  }

  private void finish(Key key, Entry entry) {
    if (!entry.cleaned || !entry.logical.isDone()) return;
    entry.finished = true;
    if (closed || entry.logical.isCompletedExceptionally() || !live(entry))
      entries.remove(key, entry);
  }

  private static RpcOperation<String> denied(String code) {
    return new RpcOperation<>(
        CompletableFuture.failedFuture(new RejectedExecutionException(code)),
        CompletableFuture.completedFuture(null));
  }

  public synchronized void invalidate() {
    epoch++;
    entries.entrySet().removeIf(e -> e.getValue().finished);
  }

  public synchronized int pending() {
    return (int) entries.values().stream().filter(e -> !e.finished).count();
  }

  public synchronized int size() {
    return entries.size();
  }

  public synchronized long retainedBytes() {
    return (long) entries.size() * ENTRY_BYTES;
  }

  public synchronized CompletionStage<Void> drain() {
    closed = true;
    invalidate();
    for (var entry : List.copyOf(entries.values()))
      entry.logical.completeExceptionally(new IllegalStateException("Relay proof cache draining"));
    if (pending() == 0) drained.complete(null);
    return drained.minimalCompletionStage();
  }
}
