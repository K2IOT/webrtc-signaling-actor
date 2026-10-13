package io.webrtc.signaling.rpc;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.ControlRouteReply;
import io.webrtc.signaling.protocol.internal.ControlRouteRequest;
import io.webrtc.signaling.storage.*;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Fresh native primary route; retained participation proves call/user provenance after release.
 */
public final class NativeControlRouteResolver {
    private final SqlTransactions sql;
    private final String cell;
    private final long epoch;
    private final Function<UserId, ProofBindings.TrustedHome> homes;
    private final HomeProofReadService.SecurityPolicy security;
    private final AcceptWinnerService.RouteSecurityPolicy routesSecurity;
    private final BooleanSupplier ready;
    private final SessionRepository sessions = new SessionRepository();

    public NativeControlRouteResolver(
            SqlTransactions sql,
            String cell,
            long epoch,
            Function<UserId, ProofBindings.TrustedHome> homes,
            HomeProofReadService.SecurityPolicy security,
            AcceptWinnerService.RouteSecurityPolicy routesSecurity,
            BooleanSupplier ready) {
        this.sql = Objects.requireNonNull(sql);
        this.cell = Objects.requireNonNull(cell);
        this.epoch = epoch;
        this.homes = Objects.requireNonNull(homes);
        this.security = Objects.requireNonNull(security);
        this.routesSecurity = Objects.requireNonNull(routesSecurity);
        this.ready = Objects.requireNonNull(ready);
    }

    public RpcOperation<ControlRouteReply> resolve(
            ControlRouteRequest request, CellRpcServer.Peer peer, Duration budget) {
        try {
            var call = new CallId(request.getCallId());
            var user = new UserId(request.getUserId());
            var key = new SessionKey(request.getIssuer(), request.getJti());
            UUID.fromString(request.getEventId());
            var home = homes.apply(user);
            if (!ready.getAsBoolean()
                    || peer == null
                    || !peer.role().equals("actor")
                    || peer.workloadId().isBlank()
                    || !peer.cell().equals(call.coordinatorCell())
                    || request.getSchemaMajor() != 1
                    || request.getSerializedSize() > 4096
                    || request.getCallVersion() < 1
                    || !cell.equals(request.getDestinationCell())
                    || request.getStorageEpoch() != epoch
                    || home == null
                    || !home.cell().equals(cell)
                    || home.storageEpoch() != epoch
                    || home.directoryEpoch() != request.getDirectoryEpoch()
                    || request.getRemainingBudgetMs() <= 0
                    || budget == null
                    || budget.isZero()
                    || budget.isNegative()) throw new AuthoritySql.FencedException();
            var work =
                    sql.submitTracked(
                            DbClass.OUTBOX,
                            budget,
                            c -> {
                                if (!ready.getAsBoolean() || !home.equals(homes.apply(user)))
                                    throw new AuthoritySql.FencedException();
                                AuthoritySql.cellBarrier(c, false);
                                AuthoritySql.validateCell(c, cell, epoch);
                                int userBucket = SessionRegistryService.bucket(user);
                                var buckets = new TreeSet<Integer>();
                                buckets.add(userBucket);
                                if (call.coordinatorCell().equals(cell)) {
                                    try (var q =
                                                 c.prepareStatement(
                                                         "SELECT authority_bucket_id FROM call_state WHERE call_id=?")) {
                                        q.setString(1, call.value());
                                        try (var r = q.executeQuery()) {
                                            if (r.next()) buckets.add(r.getInt(1));
                                        }
                                    }
                                }
                                for (int bucket : buckets) AuthoritySql.bucketBarrier(c, bucket, false);
                                for (int bucket : buckets) {
                                    try (var q =
                                                 c.prepareStatement(
                                                         "SELECT directory_epoch,status FROM bucket_authority WHERE"
                                                                 + " bucket_id=?")) {
                                        q.setInt(1, bucket);
                                        try (var r = q.executeQuery()) {
                                            if (!r.next()
                                                    || !"ACTIVE".equals(r.getString(2))
                                                    || r.getLong(1) < 1
                                                    || bucket == userBucket && r.getLong(1) != home.directoryEpoch())
                                                throw new AuthoritySql.FencedException();
                                        }
                                    }
                                }
                                AuthoritySql.userGuards(c, List.of(user.value()));
                                if (!security.current(c, user)) throw new AuthoritySql.FencedException();
                                boolean provenance;
                                try (var q =
                                             c.prepareStatement(
                                                     "SELECT coordinator_cell FROM home_participation WHERE call_id=? AND"
                                                             + " user_id=?")) {
                                    q.setString(1, call.value());
                                    q.setString(2, user.value());
                                    try (var r = q.executeQuery()) {
                                        provenance = r.next() && peer.cell().equals(r.getString(1));
                                    }
                                }
                                // The local fast path has no remote-home participation row. Immutable call parties
                                // are
                                // its native provenance; terminal call history remains usable within retention.
                                if (!provenance && call.coordinatorCell().equals(cell)) {
                                    AuthoritySql.callReadBarrier(c, call.value());
                                    try (var q =
                                                 c.prepareStatement(
                                                         "SELECT caller_user,callee_user FROM call_state WHERE call_id=?")) {
                                        q.setString(1, call.value());
                                        try (var r = q.executeQuery()) {
                                            provenance =
                                                    r.next()
                                                            && (user.value().equals(r.getString(1))
                                                            || user.value().equals(r.getString(2)));
                                        }
                                    }
                                }
                                if (!provenance) throw new AuthoritySql.FencedException();
                                var route = sessions.find(c, key);
                                if (route == null || !route.user().equals(user))
                                    throw new AuthoritySql.FencedException();
                                sessions.requireCurrent(c, route);
                                if (!routesSecurity.allowed(c, route)
                                        || !ready.getAsBoolean()
                                        || !home.equals(homes.apply(user))) throw new AuthoritySql.FencedException();
                                return route;
                            });
            // Only the immutable native Route crosses DbBoundary. Protobuf allocation/encoding belongs
            // to the transport continuation after the original admitted transaction has completed.
            var logical =
                    work.logical()
                            .thenApply(
                                    route ->
                                            ControlRouteReply.newBuilder()
                                                    .setEventId(request.getEventId())
                                                    .setCallId(call.value())
                                                    .setCallVersion(request.getCallVersion())
                                                    .setStatus("ROUTE")
                                                    .setSourceCell(cell)
                                                    .setStorageEpoch(epoch)
                                                    .setDirectoryEpoch(home.directoryEpoch())
                                                    .setRoute(ByteString.copyFrom(RpcBusinessHandler.encode(route)))
                                                    .build());
            return new RpcOperation<>(
                    logical,
                    work.physicalCompletion().thenCombine(logical.handle((v, e) -> null), (a, b) -> null));
        } catch (RuntimeException unavailable) {
            return new RpcOperation<>(
                    CompletableFuture.failedFuture(unavailable), CompletableFuture.completedFuture(null));
        }
    }
}
