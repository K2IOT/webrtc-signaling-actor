package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.admission.CompletionReceipt;
import io.webrtc.signaling.actors.admission.EntityAdmission;
import io.webrtc.signaling.actors.admission.TrackedEntityAsk;
import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.call.RecoveryWakeRegistry;
import io.webrtc.signaling.actors.cluster.ApplicationSerializer;
import io.webrtc.signaling.actors.cluster.NativeEnvelopeAdmission;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.SignalEnvelope;
import io.webrtc.signaling.storage.CallCommandService;
import io.webrtc.signaling.storage.CallWorkflowService;
import io.webrtc.signaling.storage.DbOverloadedException;
import io.webrtc.signaling.storage.HomeParticipationService;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Shared admission before EntityRef enqueue; physical receipts survive logical timeout/entity
 * termination.
 */
public final class ShardedActorIngress implements RpcBusinessHandler.ActorIngress {
    private static final Map<ActorSystem<?>, PhysicalState> PHYSICAL =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<ActorSystem<?>, EntityAdmission> ADMISSIONS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private final PhysicalState physicalState;
    private final ActorSystem<?> system;
    private final ClusterSharding sharding;
    private final Clock clock;
    private final EntityAdmission admission;
    private final ApplicationSerializer serializer;
    private volatile Consumer<String> compatibility =
            feature -> {
                throw new DbOverloadedException();
            };
    private boolean compatibilityInstalled;
    private volatile java.util.function.BooleanSupplier traceEligibility = () -> false;
    public ShardedActorIngress(ActorSystem<?> system, Clock clock) {
        this(
                system,
                clock,
                ADMISSIONS.computeIfAbsent(system, key -> EntityAdmission.forIngressProducers(7)));
    }

    public ShardedActorIngress(ActorSystem<?> system, Clock clock, EntityAdmission admission) {
        this.system = Objects.requireNonNull(system);
        physicalState = PHYSICAL.computeIfAbsent(system, key -> new PhysicalState());
        sharding = ClusterSharding.get(system);
        this.clock = Objects.requireNonNull(clock);
        this.admission = Objects.requireNonNull(admission);
        serializer = new ApplicationSerializer(Adapter.toClassic(system));
    }

    public void installTraceEligibility(java.util.function.BooleanSupplier eligibility) {
        traceEligibility = Objects.requireNonNull(eligibility);
    }

    private io.webrtc.signaling.observability.TraceContext actorTrace() {
        try {
            return traceEligibility.getAsBoolean()
                    ? io.webrtc.signaling.observability.TraceContext.current()
                    : null;
        } catch (RuntimeException unavailable) {
            return null;
        }
    }

    public synchronized void installCompatibilityGuard(Consumer<String> guard) {
        if (compatibilityInstalled)
            throw new IllegalStateException("Actor compatibility guard already installed");
        compatibility = Objects.requireNonNull(guard);
        compatibilityInstalled = true;
    }

    public synchronized boolean compatibilityInstalled() {
        return compatibilityInstalled;
    }

    private Duration remaining(Instant deadline) {
        var duration = Duration.between(clock.instant(), deadline);
        if (duration.isNegative() || duration.isZero()) throw new DbOverloadedException();
        return duration.compareTo(Duration.ofSeconds(2)) > 0 ? Duration.ofSeconds(2) : duration;
    }

    private <T> RpcOperation<T> ask(
            String entity,
            EntityAdmission.Priority priority,
            int bytes,
            Instant deadline,
            Class<T> type,
            WireSend<T> send) {
        return ask(entity, priority, bytes, deadline, type, send, witness -> {
        });
    }

    private <T> RpcOperation<T> ask(
            String entity,
            EntityAdmission.Priority priority,
            int bytes,
            Instant deadline,
            Class<T> type,
            WireSend<T> send,
            Consumer<CompletionReceipt.PhysicalOwner> witness) {
        synchronized (physicalState) {
            if (physicalState.draining) throw new EntityAdmission.Overloaded();
            int producers = 0;
            for (var member : org.apache.pekko.cluster.typed.Cluster.get(system).state().getMembers())
                if (member.hasRole("signaling-actor")) producers++;
            if (producers > 7) throw new EntityAdmission.Overloaded();
            var ticket = admission.acquire(entity, priority, bytes);
            try {
                var result =
                        TrackedEntityAsk.ask(
                                system,
                                remaining(deadline),
                                type,
                                (reply, receipt) -> send.send(reply, receipt, ticket),
                                witness);
                ticket.releaseAfter(result.physicalCompletion());
                var physical = result.physicalCompletion().toCompletableFuture();
                physicalState.active.add(physical);
                physical.whenComplete(
                        (v, error) -> {
                            if (error == null)
                                synchronized (physicalState) {
                                    physicalState.active.remove(physical);
                                }
                        });
                return new RpcOperation<>(result.logical(), result.physicalCompletion());
            } catch (RuntimeException failed) {
                ticket.close();
                throw failed;
            }
        }
    }

    public CompletionStage<Void> settleAdmitted() {
        synchronized (physicalState) {
            return CompletableFuture.allOf(physicalState.active.toArray(CompletableFuture[]::new))
                    .minimalCompletionStage();
        }
    }

    /**
     * Invoke only after framework handoff: closes all producers in this ActorSystem permanently.
     */
    public CompletionStage<Void> drain() {
        synchronized (physicalState) {
            physicalState.draining = true;
            return settleAdmitted();
        }
    }

    public CompletionStage<CallActor.GrantReply> grant(
            HomeParticipationService.Request r,
            HomeParticipationService.AuthorizationIntent action,
            String destination,
            Instant deadline,
            int bytes) {
        return grantTracked(r, action, destination, deadline, bytes).logical();
    }

    public RpcOperation<CallActor.GrantReply> grantTracked(
            HomeParticipationService.Request r,
            HomeParticipationService.AuthorizationIntent action,
            String destination,
            Instant deadline,
            int bytes) {
        var trace = actorTrace();
        return ask(
                "call:" + r.call().value(),
                Set.of("RENEW", "RELEASE", "CONFIRM").contains(action.action())
                        ? EntityAdmission.Priority.SAFETY
                        : EntityAdmission.Priority.NORMAL,
                bytes,
                deadline,
                CallActor.GrantReply.class,
                (reply, receipt, ticket) ->
                        NativeEnvelopeAdmission.send(
                                ticket,
                                charge ->
                                        new CallActor.GrantToHome(
                                                r, action, destination, reply, deadline, charge, receipt, trace),
                                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, r.call().value())::tell,
                                serializer));
    }

    /**
     * The owning CallActor runs the relay with its node-local authority; any actor may receive the
     * RPC. Without the capability on every peer the caller keeps the previous local handling.
     */
    @Override
    public RpcOperation<CallActor.RelayReply> relayTracked(
            RpcBusinessHandler.RelayRequest request, Instant deadline) {
        try {
            compatibility.accept(io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_RELAY_OWNER_V1);
        } catch (IllegalStateException missingCapability) {
            // Only a peer without the capability falls back; any other failure stays fail-closed.
            throw new RpcBusinessHandler.RelayForwardUnavailable();
        }
        var call = new io.webrtc.signaling.protocol.Identity.CallId(request.command().getCallId());
        String command = java.util.Base64.getEncoder().encodeToString(request.command().toByteArray());
        var peer = request.peer();
        return ask(
                "call:" + call.value(),
                EntityAdmission.Priority.NORMAL,
                command.length() + 512,
                deadline,
                CallActor.RelayReply.class,
                (reply, receipt, ticket) ->
                        NativeEnvelopeAdmission.send(
                                ticket,
                                charge ->
                                        new CallActor.Relay(
                                                call,
                                                command,
                                                peer.cell(),
                                                peer.role(),
                                                peer.workloadId(),
                                                reply,
                                                deadline,
                                                charge,
                                                receipt),
                                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, call.value())::tell,
                                serializer));
    }

    /**
     * Reconciliation addresses the original collector and actual hosting-process witness.
     */
    public RecoveryWake recoveryWake(
            io.webrtc.signaling.protocol.Identity.CallId call, Instant deadline) {
        compatibility.accept(io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_WAKE_RECEIPT_V1);
        compatibility.accept(
                io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_RECOVERY_RECEIPT_V1);
        var original = new AtomicReference<CompletionReceipt>();
        var provider = new AtomicReference<ActorRef<RecoveryWakeRegistry.Query>>();
        var retained = new AtomicReference<EntityAdmission.Ticket>();
        var finished = new java.util.concurrent.atomic.AtomicBoolean();
        var operation =
                ask(
                        "call:" + call.value(),
                        EntityAdmission.Priority.RECOVERY,
                        256,
                        deadline,
                        Boolean.class,
                        (reply, receipt, ticket) -> {
                            original.set(receipt);
                            retained.set(ticket);
                            NativeEnvelopeAdmission.send(
                                    ticket,
                                    charge -> new CallActor.WakeCall(call, reply, deadline, charge, receipt),
                                    sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, call.value())::tell,
                                    serializer);
                        },
                        witness -> {
                            provider.set(witness.owner());
                            if (finished.get())
                                witness.owner().tell(new RecoveryWakeRegistry.Acknowledge(call, original.get()));
                        });
        operation
                .physicalCompletion()
                .whenComplete(
                        (done, failure) -> {
                            if (failure == null) {
                                finished.set(true);
                                var owner = provider.get();
                                if (owner != null)
                                    owner.tell(new RecoveryWakeRegistry.Acknowledge(call, original.get()));
                            }
                        });
        return new RecoveryWake(
                operation,
                budget -> {
                    if (finished.get()) return;
                    var receipt = original.get();
                    if (receipt == null) return;
                    var owner = provider.get();
                    if (owner != null) owner.tell(new RecoveryWakeRegistry.Reconcile(call, receipt));
                    else {
                        compatibility.accept(
                                io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_RECOVERY_RECEIPT_V1);
                        NativeEnvelopeAdmission.send(
                                retained.get(),
                                reconcileFactory(call, receipt, clock, budget),
                                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, call.value())::tell,
                                serializer);
                    }
                });
    }

    /**
     * The envelope is built more than once while it is sized and sent, and its serialized length
     * depends on the instant: read the clock once, outside the factory.
     */
    static java.util.function.IntFunction<CallActor.ReconcileWake> reconcileFactory(
            io.webrtc.signaling.protocol.Identity.CallId call,
            CompletionReceipt receipt,
            Clock clock,
            Duration budget) {
        Instant deadline = clock.instant().plus(budget);
        return charge -> new CallActor.ReconcileWake(call, receipt, deadline);
    }

    public RpcOperation<Boolean> wakeTracked(
            io.webrtc.signaling.protocol.Identity.CallId call, Instant deadline) {
        return recoveryWake(call, deadline).operation();
    }

    public CompletionStage<UserCommand.Result> user(
            UserCommand.Operation operation, Instant deadline, int bytes) {
        return userTracked(operation, deadline, bytes).logical();
    }

    public RpcOperation<UserCommand.Result> userTracked(
            UserCommand.Operation operation, Instant deadline, int bytes) {
        var trace = actorTrace();
        if (operation instanceof UserCommand.QueryProof)
            compatibility.accept(
                    io.webrtc.signaling.protocol.RuntimeCapabilities.HOME_PROOF_HIGH_WATER_V1);
        if (operation instanceof UserCommand.Renew)
            compatibility.accept(io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_RENEWAL_V1);
        if (operation instanceof UserCommand.QueryProof proof && proof.renewalProjection())
            compatibility.accept(io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_RENEWAL_V1);
        var priority =
                operation instanceof UserCommand.Renew
                        || operation instanceof UserCommand.Release
                        || operation instanceof UserCommand.Close
                        || operation instanceof UserCommand.Refresh
                        || operation instanceof UserCommand.Activate
                        || operation instanceof UserCommand.QueryProof proof && proof.renewalProjection()
                        ? EntityAdmission.Priority.SAFETY
                        : EntityAdmission.Priority.NORMAL;
        return ask(
                "user:" + operation.user().value(),
                priority,
                bytes,
                deadline,
                UserCommand.Result.class,
                (reply, receipt, ticket) ->
                        NativeEnvelopeAdmission.send(
                                ticket,
                                charge ->
                                        new UserCommand.Mutate(operation, reply, deadline, charge, receipt, trace),
                                sharding.entityRefFor(ShardingBootstrap.USER_TYPE, operation.user().value())::tell,
                                serializer));
    }

    public CompletionStage<CallCommandService.Outcome> call(
            CallCommand command, String proof, Instant deadline, int bytes) {
        return callTracked(command, proof, deadline, bytes).logical();
    }

    public RpcOperation<CallCommandService.Outcome> callTracked(
            CallCommand command, String proof, Instant deadline, int bytes) {
        var trace = actorTrace();
        if (command.callId() == null)
            throw new IllegalArgumentException("Candidate call must be selected before shard routing");
        var priority =
                Set.of(SignalEnvelope.Type.HANGUP, SignalEnvelope.Type.CANCEL, SignalEnvelope.Type.RESUME)
                        .contains(command.type())
                        ? EntityAdmission.Priority.SAFETY
                        : EntityAdmission.Priority.NORMAL;
        return ask(
                "call:" + command.callId().value(),
                priority,
                bytes,
                deadline,
                CallCommandService.Outcome.class,
                (reply, receipt, ticket) ->
                        NativeEnvelopeAdmission.send(
                                ticket,
                                charge ->
                                        new CallActor.Execute(command, proof, reply, deadline, charge, receipt, trace),
                                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, command.callId().value())::tell,
                                serializer));
    }

    public CompletionStage<CallWorkflowService.Outcome> progress(
            CallWorkflowService.Transition transition, Instant deadline, int bytes) {
        return progressTracked(transition, deadline, bytes).logical();
    }

    public RpcOperation<CallWorkflowService.Outcome> progressTracked(
            CallWorkflowService.Transition transition, Instant deadline, int bytes) {
        var trace = actorTrace();
        return ask(
                "call:" + transition.call().value(),
                transition.step() == CallWorkflowService.Step.TERMINATE
                        ? EntityAdmission.Priority.SAFETY
                        : EntityAdmission.Priority.NORMAL,
                bytes,
                deadline,
                CallWorkflowService.Outcome.class,
                (reply, receipt, ticket) ->
                        NativeEnvelopeAdmission.send(
                                ticket,
                                charge ->
                                        new CallActor.Progress(transition, reply, deadline, charge, receipt, trace),
                                sharding.entityRefFor(ShardingBootstrap.CALL_TYPE, transition.call().value())::tell,
                                serializer));
    }

    @FunctionalInterface
    private interface WireSend<T> {
        void send(ActorRef<T> reply, CompletionReceipt receipt, EntityAdmission.Ticket ticket);
    }

    private static final class PhysicalState {
        final Set<CompletableFuture<?>> active = new HashSet<>();
        boolean draining;
    }

    public record RecoveryWake(RpcOperation<Boolean> operation, Consumer<Duration> reconciliation) {
        public void reconcile(Duration budget) {
            reconciliation.accept(budget);
        }
    }
}
