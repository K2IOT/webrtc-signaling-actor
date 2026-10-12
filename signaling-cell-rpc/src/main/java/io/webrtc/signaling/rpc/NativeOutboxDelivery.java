package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.worker.OutboxDispatcher;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Original claimed SQL -> authenticated current home route -> exact boot/application receipt. */
public final class NativeOutboxDelivery implements OutboxDispatcher.Delivery {
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(16)
                          .maxStringLength(8192)
                          .build())
                  .build())
          .findAndRegisterModules()
          .enable(
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
              DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  private final SqlTransactions sql;
  private final String cell, worker;
  private final long epoch;
  private final UUID incarnation;
  private final Function<UserId, ProofBindings.TrustedHome> homes;
  private final CellRpcClient homeRpc;
  private final GatewayRelayRpcClient gateways;
  private final BooleanSupplier ready;

  private record Verified(OutboxRepository.Claim claim, long deadline) {}

  private final Map<OutboxDispatcher.Receipt, Verified> verified = new IdentityHashMap<>();

  public NativeOutboxDelivery(
      SqlTransactions sql,
      String cell,
      long epoch,
      String worker,
      UUID incarnation,
      Function<UserId, ProofBindings.TrustedHome> homes,
      CellRpcClient homeRpc,
      GatewayRelayRpcClient gateways,
      BooleanSupplier ready) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    this.epoch = epoch;
    this.worker = Objects.requireNonNull(worker);
    this.incarnation = Objects.requireNonNull(incarnation);
    this.homes = Objects.requireNonNull(homes);
    this.homeRpc = Objects.requireNonNull(homeRpc);
    this.gateways = Objects.requireNonNull(gateways);
    this.ready = Objects.requireNonNull(ready);
  }

  /** Identity and exact dispatch generation bind production completion to this adapter's result. */
  public synchronized boolean verified(
      OutboxRepository.Claim claim, OutboxDispatcher.Receipt receipt) {
    var issued = verified.remove(receipt);
    return issued != null
        && issued.deadline() - System.nanoTime() > 0
        && issued.claim().equals(claim)
        && receipt.kind() == OutboxDispatcher.Kind.APPLICATION_RECEIVED;
  }

  @Override
  public DbOperation<OutboxDispatcher.Receipt> send(OutboxRepository.Claim claim, Duration budget) {
    final UserId user;
    final SessionKey key;
    final JsonNode metadata;
    final long end;
    try {
      if (budget == null
          || budget.isNegative()
          || budget.isZero()
          || budget.compareTo(Duration.ofSeconds(2)) > 0
          || !cell.equals(claim.callId().coordinatorCell()))
        throw new IllegalArgumentException("Invalid control delivery budget/authority");
      end = System.nanoTime() + budget.toNanos();
      var destination = JSON.readTree(claim.destination());
      if (destination == null
          || !destination.isObject()
          || destination.size() != 3
          || !destination.path("userId").isTextual()
          || !destination.path("issuer").isTextual()
          || !destination.path("jti").isTextual())
        throw new IllegalArgumentException("Malformed destination");
      user = new UserId(destination.path("userId").asText());
      key = new SessionKey(destination.path("issuer").asText(), destination.path("jti").asText());
      var payload = Base64.getDecoder().decode(claim.payloadBase64());
      if (payload.length > 8192) throw new IllegalArgumentException("Malformed metadata");
      metadata = JSON.readTree(payload);
      if (metadata == null
          || !metadata.isObject()
          || !metadata.path("type").isTextual()
          || !metadata.path("type").asText().matches("[A-Z_]{1,64}")
          || !metadata.path("callId").asText().equals(claim.callId().value())
          || !metadata.path("callVersion").asText().equals(Long.toString(claim.version())))
        throw new IllegalArgumentException("Malformed event correlation");
    } catch (Exception malformed) {
      return completed(claim, OutboxDispatcher.Kind.MALFORMED);
    }
    var scope = new PhysicalScope();
    CompletionStage<OutboxDispatcher.Receipt> logical;
    try {
      var checked =
          sql.submitTracked(
              DbClass.OUTBOX,
              remaining(end),
              c -> {
                if (!ready.getAsBoolean()) throw new AuthoritySql.FencedException();
                AuthoritySql.cellBarrier(c, false);
                AuthoritySql.validateCell(c, cell, epoch);
                AuthoritySql.bucketBarrier(c, claim.bucket(), false);
                try (var q =
                    c.prepareStatement(
                        "SELECT directory_epoch,status FROM bucket_authority WHERE bucket_id=?")) {
                  q.setInt(1, claim.bucket());
                  try (var r = q.executeQuery()) {
                    if (!r.next() || r.getLong(1) < 1 || !r.getString(2).equals("ACTIVE"))
                      throw new AuthoritySql.FencedException();
                  }
                }
                try (var q =
                    c.prepareStatement(
                        "SELECT destination,payload FROM control_outbox WHERE event_id=? AND"
                            + " authority_bucket_id=? AND call_id=? AND version=? AND"
                            + " dispatch_owner=? AND dispatch_incarnation=? AND"
                            + " dispatch_generation=? AND delivery_state='INFLIGHT' AND"
                            + " dispatch_until>clock_timestamp() AND expires_at>clock_timestamp()"
                            + " AND quarantined=false")) {
                  q.setObject(1, claim.eventId());
                  q.setInt(2, claim.bucket());
                  q.setString(3, claim.callId().value());
                  q.setLong(4, claim.version());
                  q.setString(5, worker);
                  q.setObject(6, incarnation);
                  q.setLong(7, claim.generation());
                  try (var r = q.executeQuery()) {
                    if (!r.next()
                        || !JSON.readTree(r.getString(1)).equals(JSON.readTree(claim.destination()))
                        || !Arrays.equals(
                            r.getBytes(2), Base64.getDecoder().decode(claim.payloadBase64())))
                      throw new AuthoritySql.FencedException();
                  }
                }
                return Boolean.TRUE;
              });
      logical =
          scope
              .track(new RpcOperation<>(checked.logical(), checked.physicalCompletion()))
              .thenCompose(
                  ok -> {
                    var home = Objects.requireNonNull(homes.apply(user));
                    var request =
                        ControlRouteRequest.newBuilder()
                            .setSchemaMajor(1)
                            .setDestinationCell(home.cell())
                            .setStorageEpoch(home.storageEpoch())
                            .setDirectoryEpoch(home.directoryEpoch())
                            .setEventId(claim.eventId().toString())
                            .setCallId(claim.callId().value())
                            .setCallVersion(claim.version())
                            .setUserId(user.value())
                            .setIssuer(key.issuer())
                            .setJti(key.jti())
                            .setRemainingBudgetMs(Math.max(1, remaining(end).toMillis()))
                            .build();
                    return scope
                        .track(homeRpc.controlRouteTracked(request, remaining(end)))
                        .thenCompose(
                            reply -> {
                              try {
                                if (!ready.getAsBoolean() || !home.equals(homes.apply(user)))
                                  throw new AuthoritySql.FencedException();
                                var route =
                                    JSON.readValue(
                                        reply.getRoute().toByteArray(),
                                        SessionRepository.Route.class);
                                if (!route.user().equals(user) || !route.key().equals(key))
                                  throw new AuthoritySql.FencedException();
                                var identity =
                                    SessionIdentity.newBuilder()
                                        .setUserId(user.value())
                                        .setIssuer(key.issuer())
                                        .setJti(key.jti())
                                        .setIncarnation(uuid(route.incarnation().value()))
                                        .setConnectionId(uuid(route.connectionId()))
                                        .setConnectionGeneration(route.connectionGeneration());
                                var event =
                                    ControlEvent.newBuilder()
                                        .setEventId(claim.eventId().toString())
                                        .setCallId(claim.callId().value())
                                        .setCallVersion(claim.version())
                                        .setAuthorityBucketId(claim.bucket())
                                        .setDestination(identity)
                                        .setType(metadata.path("type").asText())
                                        .setMetadata(
                                            ByteString.copyFrom(JSON.writeValueAsBytes(metadata)))
                                        .build();
                                return scope
                                    .track(
                                        gateways.sendControl(
                                            new GatewayRelayRpcClient.Target(
                                                home.cell(), route.gatewayId(), route.bootId()),
                                            event,
                                            route,
                                            remaining(end)))
                                    .thenApply(
                                        receipt -> {
                                          if (!home.equals(homes.apply(user))
                                              || !ready.getAsBoolean()
                                              || end - System.nanoTime() <= 0
                                              || !receipt.eventId().equals(claim.eventId())
                                              || !receipt.call().equals(claim.callId())
                                              || receipt.callVersion() != claim.version()
                                              || !receipt.route().equals(route))
                                            throw new CompletionException(
                                                new AuthoritySql.FencedException());
                                          var value =
                                              new OutboxDispatcher.Receipt(
                                                  claim.eventId(),
                                                  OutboxDispatcher.Kind.APPLICATION_RECEIVED);
                                          synchronized (this) {
                                            long now = System.nanoTime();
                                            verified
                                                .values()
                                                .removeIf(v -> v.deadline() - now <= 0);
                                            if (verified.size() >= 256)
                                              throw new CompletionException(
                                                  new DbOverloadedException());
                                            verified.put(
                                                value,
                                                new Verified(
                                                    claim, now + TimeUnit.SECONDS.toNanos(2)));
                                          }
                                          return value;
                                        });
                              } catch (Exception unavailable) {
                                return CompletableFuture.failedFuture(unavailable);
                              }
                            });
                  })
              .exceptionally(
                  error ->
                      new OutboxDispatcher.Receipt(
                          claim.eventId(), OutboxDispatcher.Kind.UNAVAILABLE));
    } catch (RuntimeException unavailable) {
      logical =
          CompletableFuture.completedFuture(
              new OutboxDispatcher.Receipt(claim.eventId(), OutboxDispatcher.Kind.UNAVAILABLE));
    }
    var tracked = scope.seal(logical);
    return new DbOperation<>(
        tracked.logical(),
        tracked.physicalCompletion().thenApply(v -> DbOperation.PhysicalCompletion.FINISHED));
  }

  private static Duration remaining(long end) {
    long nanos = end - System.nanoTime();
    if (nanos <= 0)
      throw new CompletionException(new TimeoutException("Original control delivery deadline"));
    return Duration.ofNanos(nanos);
  }

  private static ByteString uuid(UUID id) {
    return ByteString.copyFrom(
        ByteBuffer.allocate(16)
            .putLong(id.getMostSignificantBits())
            .putLong(id.getLeastSignificantBits())
            .array());
  }

  private static DbOperation<OutboxDispatcher.Receipt> completed(
      OutboxRepository.Claim claim, OutboxDispatcher.Kind kind) {
    return new DbOperation<>(
        CompletableFuture.completedFuture(new OutboxDispatcher.Receipt(claim.eventId(), kind)),
        CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
  }
}
