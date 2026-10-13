package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.call.ParticipantRenewal;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.CommandScope;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.GroupAuthority;
import io.webrtc.signaling.protocol.internal.InternalCommand;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.AuthorizationIntent;
import io.webrtc.signaling.storage.HomeParticipationService.Grant;
import io.webrtc.signaling.storage.HomeParticipationService.Phase;
import io.webrtc.signaling.storage.HomeParticipationService.Request;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Actor-owned native pulses. A guarded read never reserves or recreates an expired participant.
 */
public final class NativeParticipantRenewal implements ParticipantRenewal {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private final SqlTransactions sql;
    private final Function<UserId, ProofBindings.TrustedHome> homes;
    private final Supplier<NativeSagaEffects.Network> network;
    private final Clock clock;
    private volatile OperationObservation observation = OperationObservation.NONE;

    public NativeParticipantRenewal(
            SqlTransactions sql,
            Function<UserId, ProofBindings.TrustedHome> homes,
            Supplier<NativeSagaEffects.Network> network,
            Clock clock) {
        this.sql = Objects.requireNonNull(sql);
        this.homes = Objects.requireNonNull(homes);
        this.network = Objects.requireNonNull(network);
        this.clock = Objects.requireNonNull(clock);
    }

    private static Duration phaseTtl(String phase) {
        if (phase == null) return null; // Adjacent producer omitted optional source metadata.
        return switch (phase) {
            case "PREPARING" -> Duration.ofSeconds(15);
            case "RINGING", "CLAIMED", "ACTIVE" -> Duration.ofSeconds(30);
            default -> throw new AuthoritySql.FencedException();
        };
    }

    private static void validate(
            HomeProofReadService.View view,
            CallSnapshotRepository.Snapshot snapshot,
            UserId user,
            String hash) {
        var p = view.participation();
        if (view.coordinatorStorageEpoch() < 1
                || view.highestLeaseSequence() < 1
                || view.lastRenewOperation() == null
                || p.terminal()
                || p.leaseUntil() == null
                || !p.leaseUntil().isAfter(view.checkedAt())
                || !p.call().equals(snapshot.callId())
                || !p.user().equals(user)
                || !p.acquireOperation().equals(snapshot.inviteRequest().value())
                || !p.payloadHash().equals(hash)) throw new AuthoritySql.FencedException();
    }

    private static Settlement reconcile(HomeProofReadService.View view, Attempt original) {
        var p = view.participation();
        var r = original.request();
        if (!p.call().equals(r.call())
                || !p.user().equals(r.user())
                || !p.acquireOperation().equals(r.acquireOperation())
                || !p.payloadHash().equals(r.payloadHash())
                || !p.reservationId().equals(original.reservation())
                || p.version() < original.version()) throw new AuthoritySql.FencedException();
        var grant = r.grant();
        // Storage domain orders first. A lower current domain can never certify the original work.
        if (view.coordinatorStorageEpoch() < grant.storageEpoch())
            throw new AuthoritySql.FencedException();
        int tuple = Long.compare(view.coordinatorStorageEpoch(), grant.storageEpoch());
        if (tuple == 0) tuple = Long.compare(p.highestGroupEpoch(), grant.groupEpoch());
        if (tuple == 0) tuple = Long.compare(view.highestLeaseSequence(), grant.sequence());
        if (tuple == 0 && view.lastRenewOperation().equals(grant.operation())) {
            if (p.version() <= original.version()) throw new AuthoritySql.FencedException();
            return Settlement.COMMITTED;
        }
        // Physical settlement plus this guarded live projection distinguishes real supersession
        // from an original request that was never applied. A logical timeout alone proves neither.
        return tuple > 0 ? Settlement.SUPERSEDED : Settlement.NOT_APPLIED;
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null)
            failure = failure.getCause();
        return failure;
    }

    private static boolean known(Throwable failure) {
        failure = unwrap(failure);
        return failure instanceof DbOverloadedException
                || failure instanceof AuthoritySql.RetryableConflict
                || failure instanceof AuthoritySql.FencedException
                || failure instanceof HomeParticipationService.IntentConflict
                || failure instanceof IllegalArgumentException;
    }

    private static <T> RpcOperation<Optional<T>> optional(RpcOperation<T> operation) {
        return new RpcOperation<>(
                operation.logical().thenApply(Optional::of), operation.physicalCompletion());
    }

    private static <T> RpcOperation<T> rpc(DbOperation<T> operation) {
        return new RpcOperation<>(operation.logical(), operation.physicalCompletion());
    }

    private static Duration remaining(long end) {
        long left = end - System.nanoTime();
        if (left <= 0) throw new CompletionException(new TimeoutException("Renewal deadline expired"));
        return Duration.ofNanos(left);
    }

    public void observe(OperationObservation observer) {
        observation = Objects.requireNonNull(observer);
    }

    @Override
    public DbOperation<Result> renew(
            CallSnapshotRepository.Snapshot snapshot,
            AuthoritySql.GroupToken token,
            CallActor.Backend backend,
            Duration budget) {
        return run(snapshot, token, backend, budget, List.of(), false);
    }

    private DbOperation<Result> run(
            CallSnapshotRepository.Snapshot snapshot,
            AuthoritySql.GroupToken token,
            CallActor.Backend backend,
            Duration budget,
            List<Attempt> original,
            boolean reconciliation) {
        var scope = new PhysicalScope();
        // Only two fixed participants can contribute entries. Original requests remain exact across
        // UNKNOWN reconciliation; the reconciliation phase performs no participant writes.
        var attempts = new ConcurrentHashMap<UserId, Attempt>();
        original.forEach(a -> attempts.put(a.request().user(), a));
        var uncertain = new AtomicReference<Throwable>();
        long period = Duration.ofSeconds(snapshot.state().equals("PREPARING") ? 5 : 10).toNanos();
        var retryAfter = new AtomicLong(period);
        long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
        CompletionStage<Result> logical;
        try {
            var transport =
                    Objects.requireNonNull(network.get(), "Native renewal transport not installed");
            logical =
                    scope
                            .track(
                                    rpc(
                                            sql.submitTracked(
                                                    DbClass.RENEWAL,
                                                    remaining(end),
                                                    c -> {
                                                        AuthoritySql.coordinatorGrantCurrentBucket(
                                                                c, token, snapshot.bucket(), snapshot.callId().value());
                                                        var current = new CallSnapshotRepository().find(c, snapshot.callId());
                                                        if (current == null
                                                                || current.version() != snapshot.version()
                                                                || current.terminalAt() != null
                                                                || !current.caller().equals(snapshot.caller())
                                                                || !current.callee().equals(snapshot.callee()))
                                                            throw new AuthoritySql.FencedException();
                                                        boolean preparing = current.state().equals("PREPARING");
                                                        if (preparing && !current.sagaPhase().equals("RESERVE_REMOTE")
                                                                || !preparing
                                                                && !Set.of(
                                                                        "RINGING",
                                                                        "ACCEPTED",
                                                                        "ACTIVATING",
                                                                        "CONNECTING",
                                                                        "ESTABLISHED")
                                                                .contains(current.state()))
                                                            throw new AuthoritySql.FencedException();
                                                        var origin =
                                                                new CommandResultRepository()
                                                                        .find(
                                                                                c,
                                                                                current.caller().key(),
                                                                                CommandScope.invite(),
                                                                                current.inviteRequest());
                                                        if (origin == null || !current.callId().equals(origin.callId()))
                                                            throw new AuthoritySql.FencedException();
                                                        // Local creation has both reservations atomically and enters RINGING.
                                                        // Only cross-cell
                                                        // RESERVE_REMOTE can have genuinely never-acquired callee history;
                                                        // UNKNOWN remote reserve
                                                        // is resolved by the guarded live home projection below, not by
                                                        // assuming absence.
                                                        return new Plan(origin.hash(), !preparing);
                                                    })))
                            .thenCompose(
                                    plan -> {
                                        var pulses = new ArrayList<CompletableFuture<Integer>>();
                                        for (var user : List.of(snapshot.caller().user(), snapshot.callee())) {
                                            CompletableFuture<Integer> pulse;
                                            try {
                                                var home = Objects.requireNonNull(homes.apply(user));
                                                var sequence = new AtomicLong();
                                                var reads =
                                                        new NativeHomeProofClient(
                                                                grantIngress(),
                                                                transport,
                                                                clock,
                                                                projectionGrant(snapshot, token, backend, sequence));
                                                var request =
                                                        template(
                                                                snapshot,
                                                                token,
                                                                user,
                                                                home.directoryEpoch(),
                                                                plan.hash(),
                                                                UUID.randomUUID(),
                                                                snapshot.state().equals("PREPARING")
                                                                        ? Phase.PREPARING
                                                                        : Phase.RINGING);
                                                boolean required =
                                                        user.equals(snapshot.caller().user())
                                                                || plan.calleeRequired()
                                                                || attempts.containsKey(user);
                                                RpcOperation<Optional<HomeProofReadService.View>> projection =
                                                        required
                                                                ? optional(
                                                                reads.observeRenewalTracked(
                                                                        request, home.cell(), remaining(end)))
                                                                : reads.observePreparingRenewalTracked(
                                                                request, home.cell(), remaining(end));
                                                pulse =
                                                        scope
                                                                .track(projection)
                                                                .thenCompose(
                                                                        projected -> {
                                                                            if (projected.isEmpty())
                                                                                return CompletableFuture.completedFuture(0);
                                                                            var view = projected.get();
                                                                            validate(view, snapshot, user, plan.hash());
                                                                            var ttl = phaseTtl(view.reservationPhase());
                                                                            if (ttl != null)
                                                                                OperationObservation.safeRenewal(
                                                                                        observation,
                                                                                        view.checkedAt(),
                                                                                        view.participation().leaseUntil(),
                                                                                        ttl);
                                                                            var prior = attempts.get(user);
                                                                            if (reconciliation) {
                                                                                if (prior != null)
                                                                                    reconcile(view, prior);
                                                                                // A settled attempt is now classified by a fresh live
                                                                                // version/high-water read.
                                                                                // A following cycle uses the current real grant, never
                                                                                // retries an old mutation
                                                                                // with a new operation identifier or renews an expired
                                                                                // historical reservation.
                                                                                retryAfter.accumulateAndGet(
                                                                                        Duration.ofSeconds(1).toNanos(), Math::min);
                                                                                return CompletableFuture.completedFuture(0);
                                                                            }
                                                                            var participation = view.participation();
                                                                            if (token.storageEpoch() == view.coordinatorStorageEpoch()
                                                                                    && token.epoch() == participation.highestGroupEpoch()
                                                                                    && sequence.get() <= view.highestLeaseSequence()) {
                                                                                long slack =
                                                                                        Duration.between(
                                                                                                        view.checkedAt(), participation.leaseUntil())
                                                                                                .toNanos();
                                                                                if (slack <= period + Duration.ofSeconds(2).toNanos())
                                                                                    retryAfter.accumulateAndGet(
                                                                                            Duration.ofSeconds(1).toNanos(), Math::min);
                                                                                return CompletableFuture.completedFuture(0);
                                                                            }
                                                                            return pulse(
                                                                                    scope,
                                                                                    transport,
                                                                                    snapshot,
                                                                                    token,
                                                                                    backend,
                                                                                    home.cell(),
                                                                                    view,
                                                                                    plan.hash(),
                                                                                    end,
                                                                                    attempts);
                                                                        })
                                                                .toCompletableFuture();
                                            } catch (RuntimeException failure) {
                                                pulse = CompletableFuture.failedFuture(failure);
                                            }
                                            pulses.add(
                                                    pulse.whenComplete(
                                                            (v, failure) -> {
                                                                if (failure != null && !known(failure))
                                                                    uncertain.compareAndSet(null, unwrap(failure));
                                                            }));
                                        }
                                        return CompletableFuture.allOf(pulses.toArray(CompletableFuture[]::new))
                                                .thenApply(
                                                        v ->
                                                                new Result(
                                                                        pulses.stream().mapToInt(CompletableFuture::join).sum(),
                                                                        Duration.ofNanos(retryAfter.get())));
                                    });
        } catch (RuntimeException failure) {
            logical = CompletableFuture.failedFuture(failure);
        }
        logical =
                logical.handle(
                        (result, failure) -> {
                            if (failure == null) return result;
                            Throwable unknown = uncertain.get();
                            if (unknown == null && !known(failure)) unknown = unwrap(failure);
                            if (unknown != null) {
                                var exact = List.copyOf(attempts.values());
                                throw new ParticipantRenewal.Uncertain(
                                        unknown,
                                        (fresh, authority, owner, nextBudget) ->
                                                run(fresh, authority, owner, nextBudget, exact, true));
                            }
                            throw new CompletionException(unwrap(failure));
                        });
        var operation = scope.seal(logical);
        return new DbOperation<>(
                operation.logical(),
                operation.physicalCompletion().thenApply(v -> DbOperation.PhysicalCompletion.FINISHED));
    }

    private CompletionStage<Integer> pulse(
            PhysicalScope scope,
            NativeSagaEffects.Network transport,
            CallSnapshotRepository.Snapshot snapshot,
            AuthoritySql.GroupToken token,
            CallActor.Backend backend,
            String home,
            HomeProofReadService.View view,
            String hash,
            long end,
            ConcurrentHashMap<UserId, Attempt> attempts) {
        var p = view.participation();
        var request =
                template(
                        snapshot,
                        token,
                        p.user(),
                        view.directoryEpoch(),
                        hash,
                        UUID.randomUUID(),
                        snapshot.state().equals("PREPARING") ? Phase.PREPARING : Phase.RINGING);
        var action =
                new AuthorizationIntent("RENEW", p.reservationId(), p.version(), null, 0, null, null);
        return scope
                .track(rpc(backend.grant(request, action, token, snapshot.version(), remaining(end))))
                .thenCompose(
                        issued -> {
                            UUID operation =
                                    UUID.nameUUIDFromBytes(
                                            ("native-participant-renew-v1\n"
                                                    + snapshot.callId().value()
                                                    + "\n"
                                                    + p.user().value()
                                                    + "\n"
                                                    + token.storageEpoch()
                                                    + "\n"
                                                    + token.epoch()
                                                    + "\n"
                                                    + issued.sequence())
                                                    .getBytes(StandardCharsets.UTF_8));
                            var exact =
                                    template(
                                            snapshot,
                                            token,
                                            p.user(),
                                            view.directoryEpoch(),
                                            hash,
                                            operation,
                                            request.phase());
                            var signed = backend.sealGrant(exact, home, issued);
                            attempts.put(p.user(), new Attempt(signed, p.reservationId(), p.version()));
                            var g = signed.grant();
                            var authority =
                                    GroupAuthority.newBuilder()
                                            .setCellId(g.cell())
                                            .setStorageEpoch(g.storageEpoch())
                                            .setOwnershipHashVersion(Math.toIntExact(g.hashVersion()))
                                            .setGroupId(g.group())
                                            .setGroupEpoch(g.groupEpoch())
                                            .setLeaseSequence(g.sequence())
                                            .setOwnerIncarnation(
                                                    ByteString.copyFrom(
                                                            ByteBuffer.allocate(16)
                                                                    .putLong(token.incarnation().getMostSignificantBits())
                                                                    .putLong(token.incarnation().getLeastSignificantBits())
                                                                    .array()));
                            var command =
                                    InternalCommand.newBuilder()
                                            .setSchemaMajor(1)
                                            .setSchemaMinor(0)
                                            .setDestinationCell(home)
                                            .setOperationId(operation.toString())
                                            .setCallId(snapshot.callId().value())
                                            .setCommandScope(CommandScope.call(snapshot.callId()).value())
                                            .setType("RenewReservation")
                                            .setAuthority(authority)
                                            .setExpectedCallVersion(g.authorizedCallVersion())
                                            .setPayloadHash(
                                                    ByteString.copyFrom(
                                                            HexFormat.of()
                                                                    .parseHex(
                                                                            HomeParticipationService.authorizationHash(signed, action))))
                                            .setRemainingBudgetMs(Math.max(1, remaining(end).toMillis()))
                                            .setPayload(
                                                    ByteString.copyFrom(
                                                            RpcBusinessHandler.encode(
                                                                    new RpcBusinessHandler.UserPayload(
                                                                            new UserCommand.Renew(signed, p.reservationId(), p.version()),
                                                                            null))))
                                            .build();
                            return scope
                                    .track(
                                            transport.callTracked(
                                                    CellRpcServer.Operation.EXECUTE, command, remaining(end)))
                                    .thenApply(
                                            reply -> {
                                                if (!reply.getOperationId().equals(operation.toString())
                                                        || !reply.getCallId().equals(snapshot.callId().value()))
                                                    throw new DbOutcomeUnknownException();
                                                if (!reply.getAckCommitted()) {
                                                    if (reply.getErrorCode().equals("OVERLOADED"))
                                                        throw new DbOverloadedException();
                                                    if (Set.of("FENCED", "INVALID", "INTENT_CONFLICT")
                                                            .contains(reply.getErrorCode()))
                                                        throw new AuthoritySql.FencedException();
                                                    throw new DbOutcomeUnknownException();
                                                }
                                                if (!reply.getErrorCode().isEmpty())
                                                    throw new DbOutcomeUnknownException();
                                                try {
                                                    var result =
                                                            JSON.readValue(
                                                                    reply.getResult().toByteArray(), UserCommand.Result.class);
                                                    if (result.code() != UserCommand.Code.RENEWED
                                                            || result.participation() == null
                                                            || result.participation().terminal()
                                                            || !p.reservationId().equals(result.participation().reservationId())
                                                            || result.participation().version() < p.version())
                                                        throw new DbOutcomeUnknownException();
                                                    return 1;
                                                } catch (Exception invalid) {
                                                    throw new DbOutcomeUnknownException();
                                                }
                                            });
                        });
    }

    private NativeHomeProofClient.RenewalGrant projectionGrant(
            CallSnapshotRepository.Snapshot snapshot,
            AuthoritySql.GroupToken token,
            CallActor.Backend backend,
            AtomicLong sequence) {
        return (request, destination, deadline, bytes) -> {
            var issued =
                    backend.renewalProjectionGrant(
                            request, token, snapshot.version(), Duration.between(clock.instant(), deadline));
            var logical =
                    issued
                            .logical()
                            .thenApply(
                                    grant -> {
                                        sequence.set(grant.sequence());
                                        return new CallActor.GrantReply(
                                                "GRANTED", grant, backend.sealGrant(request, destination, grant));
                                    });
            return new RpcOperation<>(logical, issued.physicalCompletion());
        };
    }

    private RpcBusinessHandler.ActorIngress grantIngress() {
        return new RpcBusinessHandler.ActorIngress() {
            public CompletionStage<UserCommand.Result> user(UserCommand.Operation op, Instant d, int b) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Grant-only renewal scope"));
            }

            public CompletionStage<CallCommandService.Outcome> call(
                    CallCommand c, String p, Instant d, int b) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Grant-only renewal scope"));
            }

            public CompletionStage<CallWorkflowService.Outcome> progress(
                    CallWorkflowService.Transition t, Instant d, int b) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Grant-only renewal scope"));
            }
        };
    }

    private Request template(
            CallSnapshotRepository.Snapshot s,
            AuthoritySql.GroupToken t,
            UserId user,
            long directory,
            String hash,
            UUID operation,
            Phase phase) {
        var now = clock.instant();
        return new Request(
                user,
                s.callId(),
                s.inviteRequest().value(),
                hash,
                directory,
                phase,
                new Grant(
                        t.cell(),
                        t.storageEpoch(),
                        t.hashVersion(),
                        t.group(),
                        t.epoch(),
                        1,
                        operation,
                        now,
                        now.plusSeconds(5),
                        "UNSIGNED_NATIVE_RENEWAL",
                        s.version()));
    }

    private enum Settlement {
        COMMITTED,
        SUPERSEDED,
        NOT_APPLIED
    }

    private record Plan(String hash, boolean calleeRequired) {
    }

    private record Attempt(Request request, UUID reservation, long version) {
    }
}
