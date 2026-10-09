package io.webrtc.signaling.actors.relay;

import io.webrtc.signaling.protocol.Identity.CallId;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Generation-scoped bounded cursor at the ICE-agent boundary. Retries cannot invoke the agent
 * twice.
 */
public final class IceReceiveWindow implements AutoCloseable {
  public record Key(CallId call, long negotiationId, long iceGeneration, UUID senderIncarnation) {
    public Key {
      Objects.requireNonNull(call);
      Objects.requireNonNull(senderIncarnation);
      if (negotiationId < 1 || iceGeneration < 1)
        throw new IllegalArgumentException("Invalid ICE scope");
    }
  }

  public record Item(
      long sequence,
      String candidate,
      String sdpMid,
      Integer sdpMLineIndex,
      String usernameFragment,
      boolean end) {
    public Item {
      if (sequence < 1
          || end && candidate != null
          || !end
              && (candidate == null
                  || utf8(candidate) > 2048
                  || !validText(candidate)
                  || sdpMid == null && sdpMLineIndex == null)
          || sdpMid != null && (utf8(sdpMid) > 256 || !validText(sdpMid))
          || sdpMLineIndex != null && (sdpMLineIndex < 0 || sdpMLineIndex > 65535)
          || usernameFragment != null
              && (utf8(usernameFragment) > 256 || !validText(usernameFragment)))
        throw new IllegalArgumentException("Invalid ICE item");
    }

    @Override
    public String toString() {
      return "IceItem[sequence=" + sequence + ", end=" + end + "]";
    }
  }

  public record Range(long first, long last) {}

  public record Ack(String code, long highestContiguous, List<Range> missing) {
    public Ack {
      missing = List.copyOf(missing);
    }
  }

  private record Buffered(Item item, int bytes, RelayBufferBudget.Ticket credit) {}

  private final Key key;
  private final int maximumCandidates, maximumBytes;
  private final long deadline;
  private final LongSupplier clock;
  private final Function<Item, CompletionStage<Void>> boundary;
  private final RelayBufferBudget budget;
  private final TreeMap<Long, Buffered> waiting = new TreeMap<>();
  private final Map<Long, byte[]> hashes = new HashMap<>();
  private final List<RelayBufferBudget.Ticket> hashCredits = new ArrayList<>();
  private long contiguous, endSequence, oldest;
  private int retained;
  private boolean timerStarted, descriptionReady, ended, failed, applying;
  private String usernameFragment;
  private ScheduledFuture<?> expiry;

  public IceReceiveWindow(
      Key key,
      int candidates,
      int bytes,
      Duration age,
      LongSupplier clock,
      Function<Item, CompletionStage<Void>> boundary) {
    this(key, candidates, bytes, age, clock, boundary, RelayBufferBudget.PROCESS);
  }

  public IceReceiveWindow(
      Key key,
      int candidates,
      int bytes,
      Duration age,
      LongSupplier clock,
      Function<Item, CompletionStage<Void>> boundary,
      RelayBufferBudget budget) {
    if (candidates < 1
        || candidates > 256
        || bytes < 1
        || bytes > 262144
        || age == null
        || age.isZero()
        || age.isNegative()
        || age.compareTo(Duration.ofSeconds(10)) > 0)
      throw new IllegalArgumentException("Unsafe ICE window");
    this.key = Objects.requireNonNull(key);
    maximumCandidates = candidates;
    maximumBytes = bytes;
    deadline = age.toNanos();
    this.clock = Objects.requireNonNull(clock);
    this.boundary = Objects.requireNonNull(boundary);
    this.budget = Objects.requireNonNull(budget);
  }

  public synchronized Ack accept(Key scope, List<Item> batch) {
    if (!key.equals(scope)) return ack("STALE_GENERATION");
    if (failed) return ack("RESYNC_REQUIRED");
    if (ended) return ack("ENDED");
    if (expired()) {
      fail();
      return ack("RESYNC_REQUIRED");
    }
    try {
      if (batch == null || batch.isEmpty() || batch.size() > 20)
        throw new IllegalArgumentException();
      int batchBytes = 0;
      long previous = 0;
      for (var item : batch) {
        if (item.sequence() > maximumCandidates + (item.end() ? 1L : 0L)
            || previous != 0 && item.sequence() != previous + 1)
          throw new IllegalArgumentException();
        previous = item.sequence();
        batchBytes = Math.addExact(batchBytes, size(item));
        if (batchBytes > 8192) throw new IllegalArgumentException();
        byte[] hash = hash(item);
        var existing = hashes.get(item.sequence());
        if (existing != null && !MessageDigest.isEqual(existing, hash))
          throw new IllegalArgumentException();
        if (item.end() && endSequence != 0 && endSequence != item.sequence())
          throw new IllegalArgumentException();
        if (descriptionReady && !ufrag(item)) throw new IllegalArgumentException();
      }
      for (var item : batch) {
        if (endSequence != 0 && item.sequence() > endSequence) continue;
        if (hashes.containsKey(item.sequence())) continue;
        int bytes = size(item);
        if (bytes > maximumBytes - retained || waiting.size() >= maximumCandidates + 1)
          throw new RelayBufferBudget.Overloaded();
        var payloadCredit = budget.acquire(bytes);
        RelayBufferBudget.Ticket hashCredit;
        try {
          hashCredit = budget.acquire(64);
        } catch (RuntimeException rejected) {
          payloadCredit.close();
          throw rejected;
        }
        hashCredits.add(hashCredit);
        hashes.put(item.sequence(), hash(item));
        waiting.put(item.sequence(), new Buffered(item, bytes, payloadCredit));
        retained += bytes;
        if (item.end()) endSequence = item.sequence();
        if (!timerStarted) {
          oldest = clock.getAsLong();
          timerStarted = true;
          expiry = RelayExpiry.schedule(() -> tick(), deadline);
        }
      }
      drain();
      return ack(failed ? "RESYNC_REQUIRED" : "ACK");
    } catch (RuntimeException invalid) {
      fail();
      return ack("RESYNC_REQUIRED");
    }
  }

  public synchronized void remoteDescriptionReady(String ufrag) {
    if (failed || ended) return;
    if (ufrag == null || ufrag.isBlank() || utf8(ufrag) > 256) {
      fail();
      return;
    }
    if (descriptionReady && !Objects.equals(usernameFragment, ufrag)) {
      fail();
      return;
    }
    usernameFragment = ufrag;
    descriptionReady = true;
    for (var value : waiting.values())
      if (!ufrag(value.item())) {
        fail();
        return;
      }
    if (expired()) {
      fail();
      return;
    }
    drain();
  }

  public synchronized void reconnected() {
    /* Preserve the original gap/description deadline and cursor. */
  }

  private void drain() {
    if (failed || ended || applying || !descriptionReady) return;
    var next = waiting.get(contiguous + 1);
    if (next == null) {
      return;
    }
    applying = true;
    try {
      boundary.apply(next.item()).whenComplete((done, error) -> applied(next, error));
    } catch (RuntimeException error) {
      applied(next, error);
    }
  }

  private synchronized void applied(Buffered item, Throwable error) {
    if (waiting.remove(item.item().sequence(), item)) {
      retained -= item.bytes();
      item.credit().close();
    }
    applying = false;
    if (error != null) {
      fail();
      return;
    }
    if (failed) return;
    contiguous = item.item().sequence();
    if (item.item().end()) {
      ended = true;
      if (expiry != null) expiry.cancel(false);
      releaseQueued();
      releaseHashes();
      return;
    }
    drain();
  }

  private boolean ufrag(Item item) {
    return item.usernameFragment() == null
        || Objects.equals(item.usernameFragment(), usernameFragment);
  }

  private boolean expired() {
    return timerStarted && clock.getAsLong() - oldest >= deadline;
  }

  public synchronized Ack tick() {
    if (!ended && expired()) fail();
    return ack(failed ? "RESYNC_REQUIRED" : ended ? "ENDED" : "ACK");
  }

  private Ack ack(String code) {
    var missing = new ArrayList<Range>();
    long expected = contiguous + 1;
    for (long sequence : waiting.keySet()) {
      if (sequence > expected && missing.size() < 8) missing.add(new Range(expected, sequence - 1));
      expected = sequence + 1;
    }
    return new Ack(code, contiguous, missing);
  }

  private void fail() {
    failed = true;
    if (expiry != null) expiry.cancel(false);
    releaseQueued();
    releaseHashes();
  }

  private void releaseQueued() {
    var iterator = waiting.entrySet().iterator();
    while (iterator.hasNext()) {
      var item = iterator.next();
      if (applying && item.getKey() == contiguous + 1) continue;
      item.getValue().credit().close();
      retained -= item.getValue().bytes();
      iterator.remove();
    }
  }

  private void releaseHashes() {
    hashes.clear();
    hashCredits.forEach(RelayBufferBudget.Ticket::close);
    hashCredits.clear();
  }

  private static boolean validText(String value) {
    return value.codePoints().noneMatch(c -> c >= 0xd800 && c <= 0xdfff);
  }

  private static int utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static int size(Item item) {
    return 96
        + (item.candidate() == null ? 0 : utf8(item.candidate()))
        + (item.sdpMid() == null ? 0 : utf8(item.sdpMid()))
        + (item.usernameFragment() == null ? 0 : utf8(item.usernameFragment()));
  }

  private static byte[] hash(Item item) {
    try {
      var bytes = new java.io.ByteArrayOutputStream();
      var out = new java.io.DataOutputStream(bytes);
      out.writeLong(item.sequence());
      out.writeBoolean(item.end());
      field(out, item.candidate());
      field(out, item.sdpMid());
      out.writeInt(item.sdpMLineIndex() == null ? -1 : item.sdpMLineIndex());
      field(out, item.usernameFragment());
      return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
    } catch (java.io.IOException | NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static void field(java.io.DataOutputStream out, String value) throws java.io.IOException {
    if (value == null) {
      out.writeInt(-1);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  public synchronized long highestContiguous() {
    return contiguous;
  }

  public synchronized boolean ended() {
    return ended;
  }

  public synchronized int retainedBytes() {
    return retained;
  }

  @Override
  public synchronized void close() {
    fail();
  }
}
