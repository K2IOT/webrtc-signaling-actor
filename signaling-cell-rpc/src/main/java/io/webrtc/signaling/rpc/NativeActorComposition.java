package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.call.CallCommandHandler;
import io.webrtc.signaling.actors.call.CallRecoveryService;
import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider;
import io.webrtc.signaling.actors.user.PostgresUserBackend;
import io.webrtc.signaling.actors.user.UserActor;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.CallAuthorizationPolicy;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.SignalEnvelope;
import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Adapter;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One actor pod: installed native roots, genuine User/Call backends, EntityRef ingress and RPC
 * producers.
 */
public final class NativeActorComposition {
    private final ActorSystem<?> system;
    private final Inputs inputs;
    private final ClusterReadiness readiness;
    private final ShardedActorIngress ingress;
    private final NativeProofIssuer issuer;
    private final ProofBindings bindings;
    private final SessionRegistryService sessions;
    private final PostgresUserBackend users;
    private final CallCommandService commands;
    private final CallWorkflowService workflow;
    private final CoordinatorGrantService grants;
    private final NativeParticipantRenewal participantRenewal;
    private volatile io.webrtc.signaling.observability.OperationObservation observation =
            io.webrtc.signaling.observability.OperationObservation.NONE;
    private boolean registered;
    private volatile NativeRelayProducer relayProducer;
    private volatile NativeSagaEffects.Network renewalNetwork;
    private volatile RecoveryOutcomeSink recoveryOutcomes = RecoveryOutcomeSink.NONE;
    private volatile io.webrtc.signaling.protocol.RuntimeCapabilities runtimeCapabilities =
            io.webrtc.signaling.protocol.RuntimeCapabilities.DENY;

    public NativeActorComposition(ActorSystem<?> system, Inputs inputs, ClusterReadiness readiness) {
        this.system = Objects.requireNonNull(system);
        this.inputs = Objects.requireNonNull(inputs);
        this.readiness = Objects.requireNonNull(readiness);
        var classic = Adapter.toClassic(system);
        var roots = new GroupOwnerRepository(inputs.sql(), inputs.cell(), inputs.storageEpoch());
        PostgresShardLeaseProvider.install(
                classic,
                roots,
                inputs.cell(),
                inputs.storageEpoch(),
                inputs.podUid(),
                inputs.trustedClock());
        bindings = new ProofBindings(inputs.proofs(), inputs.clock());
        ingress = new ShardedActorIngress(system, inputs.clock());
        var home =
                new HomeParticipationService(
                        inputs.sql(),
                        inputs.cell(),
                        inputs.storageEpoch(),
                        bindings.homeVerifier(inputs.cell()),
                        inputs.epochAdoption());
        sessions =
                new SessionRegistryService(
                        inputs.sql(), inputs.cell(), inputs.storageEpoch(), inputs.sessionSecurity());
        users =
                new PostgresUserBackend(
                        new UserSnapshotService(inputs.sql(), inputs.cell(), inputs.storageEpoch()),
                        sessions,
                        new UserReservationService(home),
                        new AcceptWinnerService(home, inputs.routeSecurity()),
                        new HomeActivationService(home, inputs.homeSecurity()),
                        new HomeProofReadService(home, inputs.homeSecurity(), inputs.routeSecurity()));
        commands =
                new CallCommandService(
                        inputs.sql(),
                        inputs.cell(),
                        inputs.storageEpoch(),
                        inputs.routingEpoch(),
                        c -> {
                            throw new IllegalStateException("Commands require hosting EntityRef authority");
                        },
                        bindings.commandVerifier(inputs.cell(), inputs.homes()),
                        bindings.negotiationVerifier(inputs.cell(), inputs.homes()),
                        inputs.timers())
                        .businessAdmission(readiness::businessReady);
        String owner = PostgresShardLeaseProvider.ownerNode(classic);
        workflow =
                new CallWorkflowService(
                        inputs.sql(),
                        inputs.cell(),
                        inputs.storageEpoch(),
                        owner,
                        bindings.workflowVerifier(inputs.cell(), inputs.homes()),
                        loss -> null,
                        inputs.timers());
        grants = new CoordinatorGrantService(inputs.sql(), inputs.cell(), inputs.storageEpoch(), owner);
        participantRenewal =
                new NativeParticipantRenewal(
                        inputs.sql(), inputs.homes(), () -> renewalNetwork, inputs.clock());
        issuer =
                new NativeProofIssuer(
                        inputs.proofs(),
                        inputs.clock(),
                        inputs.trustedClock(),
                        token -> local(token.group()).filter(token::equals).isPresent());
    }

    private Optional<AuthoritySql.GroupToken> local(int group) {
        if (!inputs.trustedClock().getAsBoolean() || !readiness.safetyReady()) return Optional.empty();
        return PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(system), group)
                .map(GroupOwnerRepository.Grant::token);
    }

    private ProofBindings.TrustedHome localHome(UserId user) {
        var home = Objects.requireNonNull(inputs.homes().apply(user));
        if (!inputs.cell().equals(home.cell()) || home.storageEpoch() != inputs.storageEpoch())
            throw new AuthoritySql.FencedException();
        return home;
    }

    public synchronized ShardingBootstrap.Regions register() {
        if (registered) throw new IllegalStateException("Native regions already registered");
        var regions =
                ShardingBootstrap.registerBoth(
                        system,
                        entity -> {
                            var user = new UserId(entity.getEntityId());
                            return UserActor.create(
                                    user, localHome(user).directoryEpoch(), entity.getShard(), users, inputs.clock());
                        },
                        UserCommand.Stop.INSTANCE,
                        entity -> {
                            var call = new CallId(entity.getEntityId());
                            int group = HomeParticipationService.group(call);
                            Supplier<Optional<AuthoritySql.GroupToken>> gate = () -> local(group);
                            var backend =
                                    new CallCommandHandler(
                                            workflow,
                                            commands,
                                            gate,
                                            user -> {
                                                var home = Objects.requireNonNull(inputs.homes().apply(user));
                                                return new CallCommandService.TargetHome(
                                                        home.cell(), home.directoryEpoch());
                                            },
                                            grants,
                                            issuer::coordinator,
                                            command ->
                                                    command.type() == SignalEnvelope.Type.INVITE
                                                            ? localHome(command.sender().userId()).directoryEpoch()
                                                            : 0L);
                            return CallActor.create(
                                    call,
                                    backend,
                                    gate,
                                    inputs.clock(),
                                    entity.getShard(),
                                    changed -> {
                                        var producer = relayProducer;
                                        if (producer != null) producer.invalidate(changed);
                                    },
                                    participantRenewal);
                        },
                        CallActor.Stop.INSTANCE,
                        readiness);
        registered = true;
        return regions;
    }

    public RpcBusinessHandler backend(
            NativeSagaEffects.Network network,
            Function<
                    RpcBusinessHandler.RelayRequest,
                    CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply>>
                    relay) {
        renewalNetwork = Objects.requireNonNull(network);
        var handler =
                new RpcBusinessHandler(
                        inputs.cell(),
                        ingress,
                        bindings,
                        inputs.proofs(),
                        inputs.homes(),
                        read -> {
                            throw new IllegalStateException("Native reads are mandatory");
                        },
                        relay,
                        inputs.clock(),
                        issuer)
                        .nativeReads(new NativeSnapshotReads(commands))
                        .businessAdmission(readiness::businessReady);
        var homeProofs =
                new NativeHomeProofClient(ingress, Objects.requireNonNull(network), inputs.clock());
        return handler
                .nativeSetup(
                        new NativeSetupCommandExecutor(
                                commands,
                                homeProofs,
                                ingress,
                                network,
                                inputs.homes(),
                                inputs.cell(),
                                inputs.storageEpoch(),
                                inputs.clock()))
                .nativeCritical(
                        new NativeCriticalCommandExecutor(
                                commands,
                                homeProofs,
                                ingress,
                                inputs.homes(),
                                inputs.cell(),
                                inputs.storageEpoch(),
                                inputs.clock()));
    }

    /**
     * Install at the actual hosting pod; a nonhosting process cannot refresh relay authority.
     */
    public NativeRelayAuthorization relayAuthorization(NativeSagaEffects.Network network) {
        return new NativeRelayAuthorization(
                commands,
                new NativeHomeProofClient(ingress, Objects.requireNonNull(network), inputs.clock()),
                inputs.proofs(),
                inputs.homes(),
                call -> local(HomeParticipationService.group(call)),
                inputs.clock(),
                () -> inputs.trustedClock().getAsBoolean() && readiness.safetyReady())
                .recoveryOutcomes(() -> recoveryOutcomes);
    }

    /**
     * Caller owns lifecycle; install with backend.nativeRelay before opening the RPC listener.
     */
    public synchronized NativeRelayProducer relayProducer(
            NativeSagaEffects.Network network,
            int capacity,
            io.webrtc.signaling.actors.relay.RelayBufferBudget memory,
            GatewayRelayRpcClient gateway) {
        if (relayProducer != null)
            throw new IllegalStateException("Native relay producer already created");
        Objects.requireNonNull(gateway);
        Objects.requireNonNull(memory);
        BooleanSupplier trusted = () -> inputs.trustedClock().getAsBoolean() && readiness.safetyReady();
        Function<CallId, Optional<AuthoritySql.GroupToken>> owner =
                call -> local(HomeParticipationService.group(call));
        var rounds =
                new NativeRelayRoundCache(
                        capacity,
                        Math.min(capacity, 64),
                        relayAuthorization(network),
                        System::nanoTime,
                        trusted,
                        owner);
        relayProducer =
                new NativeRelayProducer(
                        rounds, capacity, System::nanoTime, trusted, owner, memory, gateway::send)
                        .initialIceReceipts(
                                cell ->
                                        runtimeCapabilities.permits(
                                                cell,
                                                io.webrtc.signaling.protocol.RuntimeCapabilities
                                                        .NATIVE_INITIAL_ICE_RECEIPT_V1));
        return relayProducer;
    }

    /**
     * Transfers gateway ownership to the installed listener; bind only after native region
     * registration.
     */
    public synchronized NativeActorRpcIngress rpcIngress(
            String environment,
            int port,
            io.netty.handler.ssl.SslContext tls,
            RpcAdmission admission,
            NativeSagaEffects.Network network,
            int capacity,
            io.webrtc.signaling.actors.relay.RelayBufferBudget memory,
            GatewayRelayRpcClient gateway,
            BiPredicate<CellRpcServer.Peer, NativeSessionHandler.GatewayIdentity> gatewayWorkloads,
            Function<
                    io.webrtc.signaling.protocol.internal.ControlEvent,
                    CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply>>
                    deliver) {
        if (!registered) throw new IllegalStateException("Native actor regions not installed");
        if (environment == null || !environment.matches("[a-z0-9-]{1,32}") || port < 0 || port > 65535)
            throw new IllegalArgumentException("Invalid native listener identity");
        Objects.requireNonNull(tls);
        Objects.requireNonNull(admission);
        Objects.requireNonNull(network);
        Objects.requireNonNull(memory);
        Objects.requireNonNull(gateway);
        Objects.requireNonNull(gatewayWorkloads);
        var sessions =
                new NativeSessionHandler(
                        inputs.cell(), inputs.storageEpoch(), gatewayWorkloads, sessionOperations());
        var backend =
                backend(
                        network,
                        request ->
                                CompletableFuture.failedFuture(
                                        new IllegalStateException("Native relay installation required")));
        var producer = relayProducer(network, capacity, memory, gateway);
        backend.nativeRelay(producer);
        var server =
                new CellRpcServer(
                        inputs.cell(),
                        environment,
                        port,
                        tls,
                        admission,
                        backend,
                        event ->
                                CompletableFuture.completedFuture(
                                        io.webrtc.signaling.protocol.internal.InternalReply.newBuilder()
                                                .setOperationId(event.getEventId())
                                                .setCallId(event.getCallId())
                                                .setStatus("REJECTED")
                                                .setErrorCode("UNSUPPORTED_OPERATION")
                                                .build()))
                        .sessions(sessions)
                        .controlRoutes(controlRouteResolver());
        return new NativeActorRpcIngress(server, producer, gateway);
    }

    public NativeControlRouteResolver controlRouteResolver() {
        return new NativeControlRouteResolver(
                inputs.sql(),
                inputs.cell(),
                inputs.storageEpoch(),
                inputs.homes(),
                inputs.homeSecurity(),
                inputs.routeSecurity(),
                () -> inputs.trustedClock().getAsBoolean() && readiness.safetyReady());
    }

    public NativeOutboxDelivery outboxDelivery(
            CellRpcClient homeRpc,
            GatewayRelayRpcClient gateways,
            String worker,
            UUID workerIncarnation) {
        return new NativeOutboxDelivery(
                inputs.sql(),
                inputs.cell(),
                inputs.storageEpoch(),
                worker,
                workerIncarnation,
                inputs.homes(),
                homeRpc,
                gateways,
                () -> inputs.trustedClock().getAsBoolean() && readiness.safetyReady())
                .recoveryOutcomes(() -> recoveryOutcomes);
    }

    public void installRecoveryOutcomes(RecoveryOutcomeSink sink) {
        recoveryOutcomes = Objects.requireNonNull(sink);
        commands.recoveryOutcomes(sink);
    }

    public Optional<AuthoritySql.GroupToken> recoveryAuthority(CallId call) {
        return inputs.trustedClock().getAsBoolean() && readiness.safetyReady()
                ? local(HomeParticipationService.group(call))
                : Optional.empty();
    }

    public NativeSessionOperations sessionOperations() {
        return new NativeSessionOperations(
                sessions,
                inputs.tokenVerifier(),
                inputs.clock(),
                inputs.proofs().sessionProofs(),
                inputs.proofs().relaySessionProofs(),
                inputs.trustedClock(),
                inputs.callPolicy());
    }

    /**
     * Concrete mandatory worker dependencies share this composition's database and identity.
     */
    public SqlTransactions transactions() {
        return inputs.sql();
    }

    public String cell() {
        return inputs.cell();
    }

    public long storageEpoch() {
        return inputs.storageEpoch();
    }

    public String ownerNode() {
        return PostgresShardLeaseProvider.ownerNode(Adapter.toClassic(system));
    }

    public boolean participantRenewalInstalled() {
        return registered && renewalNetwork != null;
    }

    /**
     * Live local ownership plus an even partition of missing-owner discovery across Up hosts.
     */
    public Set<Integer> recoveryGroups() {
        if (!inputs.trustedClock().getAsBoolean() || !readiness.safetyReady()) return Set.of();
        var members = new ArrayList<String>();
        for (var member : org.apache.pekko.cluster.typed.Cluster.get(system).state().getMembers())
            if (member.hasRole("signaling-actor")
                    && member.status().equals(org.apache.pekko.cluster.MemberStatus.up()))
                members.add(member.uniqueAddress().toString());
        members.sort(String::compareTo);
        String self =
                org.apache.pekko.cluster.typed.Cluster.get(system).selfMember().uniqueAddress().toString();
        int index = members.indexOf(self);
        if (index < 0 || members.isEmpty()) return Set.of();
        var groups = new HashSet<Integer>();
        for (int group = 0; group < 1024; group++)
            if (group % members.size() == index || local(group).isPresent()) groups.add(group);
        return Set.copyOf(groups);
    }

    public CallRecoveryService recoveryService() {
        return new CallRecoveryService(
                inputs.sql(),
                inputs.cell(),
                inputs.storageEpoch(),
                ownerNode(),
                this::recoveryGroups,
                new CallRecoveryService.Wake() {
                    private final Map<DbOperation<Boolean>, ShardedActorIngress.RecoveryWake> originals =
                            new java.util.concurrent.ConcurrentHashMap<>();

                    public DbOperation<Boolean> apply(CallId call, Duration budget) {
                        var capturedGroup = recoveryAuthority(call);
                        var original = ingress.recoveryWake(call, inputs.clock().instant().plus(budget));
                        var logical =
                                original
                                        .operation()
                                        .logical()
                                        .thenApply(
                                                hydrated -> {
                                                    if (Boolean.TRUE.equals(hydrated)
                                                            && capturedGroup.isPresent()
                                                            && capturedGroup.equals(recoveryAuthority(call))) {
                                                        var sink = recoveryOutcomes;
                                                        RecoveryOutcomeSink.safely(
                                                                sink, () -> sink.hydrated(call, capturedGroup.get()));
                                                    }
                                                    return hydrated;
                                                });
                        var operation =
                                new DbOperation<>(
                                        logical,
                                        original
                                                .operation()
                                                .physicalCompletion()
                                                .thenApply(v -> DbOperation.PhysicalCompletion.FINISHED));
                        originals.put(operation, original);
                        operation
                                .physicalCompletion()
                                .whenComplete(
                                        (done, failure) -> {
                                            if (failure == null) originals.remove(operation, original);
                                        });
                        return operation;
                    }

                    public void reconcile(CallId call, DbOperation<Boolean> operation, Duration budget) {
                        var original = originals.get(operation);
                        if (original != null) original.reconcile(budget);
                    }
                })
                .observe(observation);
    }

    public void shedNewAcquisition() {
        PostgresShardLeaseProvider.shedNewAcquisition(Adapter.toClassic(system));
    }

    /**
     * Invoke after framework handoff/leave, while the native database remains available.
     */
    public CompletionStage<Void> drainRoots() {
        return PostgresShardLeaseProvider.drain(Adapter.toClassic(system));
    }

    /**
     * Original admitted cryptographic work must retire before the native DB pools.
     */
    public CompletionStage<Void> drainVerification() {
        return inputs.tokenVerifier().drain();
    }

    public ActorSystem<?> system() {
        return system;
    }

    public ClusterReadiness readiness() {
        return readiness;
    }

    public void installRuntimeCapabilities(
            io.webrtc.signaling.protocol.RuntimeCapabilities capabilities) {
        runtimeCapabilities = Objects.requireNonNull(capabilities);
        ingress.installCompatibilityGuard(feature -> capabilities.require(inputs.cell(), feature));
        ingress.installTraceEligibility(
                () ->
                        capabilities.permits(
                                inputs.cell(),
                                io.webrtc.signaling.protocol.RuntimeCapabilities.NATIVE_ACTOR_TRACE_V1));
        workflow.compatibility(capabilities);
    }

    public void installObservation(io.webrtc.signaling.observability.OperationObservation observer) {
        observation = Objects.requireNonNull(observer);
        participantRenewal.observe(observer);
    }

    public ShardedActorIngress ingress() {
        return ingress;
    }

    public record Inputs(
            SqlTransactions sql,
            String cell,
            long storageEpoch,
            long routingEpoch,
            UUID podUid,
            HomeAuthorizationProof proofs,
            Function<UserId, ProofBindings.TrustedHome> homes,
            SessionRegistryService.NativeSecurityPolicy sessionSecurity,
            HomeProofReadService.SecurityPolicy homeSecurity,
            HomeParticipationService.EpochAdoptionVerifier epochAdoption,
            BoundedTokenVerifier tokenVerifier,
            CallAuthorizationPolicy callPolicy,
            Clock clock,
            BooleanSupplier trustedClock,
            AcceptWinnerService.RouteSecurityPolicy routeSecurity,
            io.webrtc.signaling.storage.CallTimers timers) {
        public Inputs(
                SqlTransactions sql,
                String cell,
                long storageEpoch,
                long routingEpoch,
                UUID podUid,
                HomeAuthorizationProof proofs,
                Function<UserId, ProofBindings.TrustedHome> homes,
                SessionRegistryService.NativeSecurityPolicy sessionSecurity,
                HomeProofReadService.SecurityPolicy homeSecurity,
                HomeParticipationService.EpochAdoptionVerifier epochAdoption,
                BoundedTokenVerifier tokenVerifier,
                CallAuthorizationPolicy callPolicy,
                Clock clock,
                BooleanSupplier trustedClock,
                AcceptWinnerService.RouteSecurityPolicy routeSecurity) {
            this(
                    sql,
                    cell,
                    storageEpoch,
                    routingEpoch,
                    podUid,
                    proofs,
                    homes,
                    sessionSecurity,
                    homeSecurity,
                    epochAdoption,
                    tokenVerifier,
                    callPolicy,
                    clock,
                    trustedClock,
                    routeSecurity,
                    io.webrtc.signaling.storage.CallTimers.V1);
        }

        public Inputs {
            Objects.requireNonNull(timers);
            if (cell == null
                    || !cell.matches("[a-z][a-z0-9-]{0,23}")
                    || storageEpoch < 1
                    || routingEpoch < 1) throw new IllegalArgumentException("Invalid native actor identity");
            Objects.requireNonNull(sql);
            Objects.requireNonNull(podUid);
            Objects.requireNonNull(proofs);
            Objects.requireNonNull(homes);
            Objects.requireNonNull(routeSecurity);
            Objects.requireNonNull(sessionSecurity);
            Objects.requireNonNull(homeSecurity);
            Objects.requireNonNull(epochAdoption);
            Objects.requireNonNull(tokenVerifier);
            Objects.requireNonNull(callPolicy);
            Objects.requireNonNull(clock);
            Objects.requireNonNull(trustedClock);
            if (!cell.equals(proofs.sourceCell()))
                throw new IllegalArgumentException("Signer cell differs from native authority");
        }
    }
}
