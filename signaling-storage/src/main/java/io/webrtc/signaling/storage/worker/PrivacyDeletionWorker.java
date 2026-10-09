package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Detach optional data by an opaque keyed identifier. Native fencing/replay records are preserved.
 */
public final class PrivacyDeletionWorker {
  @FunctionalInterface
  public interface Detachment {
    DbOperation<Void> detach(String opaqueSubject);
  }

  private record Claim(String opaque, int bucket, long generation) {}

  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;
  private final byte[] key;
  private final Detachment detachment;
  private final UUID incarnation = UUID.randomUUID();
  private final AtomicBoolean busy = new AtomicBoolean();

  public PrivacyDeletionWorker(
      SqlTransactions sql, String cell, long epoch, byte[] pseudonymKey, Detachment detachment) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    this.epoch = epoch;
    if (pseudonymKey == null || pseudonymKey.length < 32 || pseudonymKey.length > 64)
      throw new IllegalArgumentException("Privacy pseudonym key required");
    key = pseudonymKey.clone();
    this.detachment = Objects.requireNonNull(detachment);
  }

  /**
   * The control plane authenticates/authorizes deletion before this internal API. No raw identity
   * is journaled.
   */
  public DbOperation<String> request(String issuer, UserId user, UUID request) {
    new SessionKey(issuer, "privacy");
    Objects.requireNonNull(request);
    String opaque = pseudonym(issuer, user);
    int bucket = SessionRegistryService.bucket(user);
    return sql.submitTracked(
        DbClass.MAINTENANCE,
        Duration.ofSeconds(2),
        c -> {
          WorkerFence.cell(c, cell, epoch);
          WorkerFence.bucket(c, bucket);
          try (var q =
              c.prepareStatement(
                  "INSERT INTO privacy_deletion_request(opaque_subject,authority_bucket_id,request_id,requested_at) VALUES(decode(?,'hex'),?,?,clock_timestamp()) ON CONFLICT DO NOTHING")) {
            q.setString(1, opaque);
            q.setInt(2, bucket);
            q.setObject(3, request);
            q.executeUpdate();
          }
          return opaque;
        });
  }

  public CompletionStage<Integer> process(int limit) {
    WorkerFence.limit(limit, 128);
    if (!busy.compareAndSet(false, true))
      return CompletableFuture.failedFuture(new DbOverloadedException());
    var work = new WorkerCompletion();
    CompletionStage<Integer> logical;
    try {
      logical =
          work.track(
                  sql.submitTracked(
                      DbClass.MAINTENANCE,
                      Duration.ofSeconds(2),
                      c -> {
                        WorkerFence.cell(c, cell, epoch);
                        var hints = new TreeSet<Integer>();
                        try (var q =
                            c.prepareStatement(
                                "SELECT p.authority_bucket_id FROM privacy_deletion_request p JOIN bucket_authority b ON b.bucket_id=p.authority_bucket_id AND b.status='ACTIVE' WHERE p.completed_at IS NULL AND (p.dispatch_until IS NULL OR p.dispatch_until<clock_timestamp()) ORDER BY p.requested_at,p.opaque_subject LIMIT ?")) {
                          q.setInt(1, limit);
                          try (var r = q.executeQuery()) {
                            while (r.next()) hints.add(r.getInt(1));
                          }
                        }
                        for (int bucket : hints) WorkerFence.bucket(c, bucket);
                        var claims = new ArrayList<Claim>();
                        var buckets = c.createArrayOf("integer", hints.toArray(Integer[]::new));
                        try (var q =
                            c.prepareStatement(
                                "WITH due AS (SELECT opaque_subject FROM privacy_deletion_request WHERE authority_bucket_id=ANY(?) AND completed_at IS NULL AND (dispatch_until IS NULL OR dispatch_until<clock_timestamp()) ORDER BY requested_at,opaque_subject LIMIT ? FOR UPDATE SKIP LOCKED) UPDATE privacy_deletion_request p SET dispatch_incarnation=?,dispatch_generation=p.dispatch_generation+1,dispatch_until=clock_timestamp()+interval '5 seconds' FROM due d WHERE p.opaque_subject=d.opaque_subject RETURNING encode(p.opaque_subject,'hex'),p.authority_bucket_id,p.dispatch_generation")) {
                          q.setArray(1, buckets);
                          q.setInt(2, limit);
                          q.setObject(3, incarnation);
                          try (var r = q.executeQuery()) {
                            while (r.next())
                              claims.add(new Claim(r.getString(1), r.getInt(2), r.getLong(3)));
                          }
                        } finally {
                          buckets.free();
                        }
                        return List.copyOf(claims);
                      }))
              .thenCompose(
                  claims -> {
                    var tasks = new ArrayList<CompletableFuture<Boolean>>();
                    for (var claim : claims)
                      try {
                        tasks.add(
                            work.track(detachment.detach(claim.opaque()))
                                .thenCompose(
                                    detached ->
                                        work.track(
                                            sql.submitTracked(
                                                DbClass.MAINTENANCE,
                                                Duration.ofSeconds(2),
                                                c -> {
                                                  WorkerFence.cell(c, cell, epoch);
                                                  WorkerFence.bucket(c, claim.bucket());
                                                  try (var q =
                                                      c.prepareStatement(
                                                          "UPDATE privacy_deletion_request SET completed_at=clock_timestamp() WHERE opaque_subject=decode(?,'hex') AND dispatch_incarnation=? AND dispatch_generation=? AND dispatch_until>clock_timestamp() AND completed_at IS NULL")) {
                                                    q.setString(1, claim.opaque());
                                                    q.setObject(2, incarnation);
                                                    q.setLong(3, claim.generation());
                                                    return q.executeUpdate() == 1;
                                                  }
                                                })))
                                .exceptionally(error -> false)
                                .toCompletableFuture());
                      } catch (RuntimeException unavailable) {
                        tasks.add(CompletableFuture.completedFuture(false));
                      }
                    return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new))
                        .thenApply(
                            done -> (int) tasks.stream().filter(CompletableFuture::join).count());
                  });
    } catch (RuntimeException failure) {
      logical = CompletableFuture.failedFuture(failure);
    }
    return work.finish(logical, () -> busy.set(false));
  }

  private String pseudonym(String issuer, UserId user) {
    try {
      var mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      byte[] a = issuer.getBytes(java.nio.charset.StandardCharsets.UTF_8),
          b = user.value().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      mac.update(java.nio.ByteBuffer.allocate(4).putInt(a.length).array());
      mac.update(a);
      mac.update(java.nio.ByteBuffer.allocate(4).putInt(b.length).array());
      return HexFormat.of().formatHex(mac.doFinal(b));
    } catch (java.security.GeneralSecurityException failure) {
      throw new IllegalStateException("Privacy pseudonymization unavailable");
    }
  }
}
