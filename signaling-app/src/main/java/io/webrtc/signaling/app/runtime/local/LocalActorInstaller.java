package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.actors.relay.RelayBufferBudget;
import io.webrtc.signaling.app.CompatibilityWindow;
import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.MountedNativeContract;
import io.webrtc.signaling.app.runtime.NativeActorBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorMaintenanceEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorProcess;
import io.webrtc.signaling.app.runtime.NativeActorProcessEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSchedulingEnrollment;
import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWalEnrollment;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.CellRpcServer;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.NativeSagaEffects;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.rpc.RpcAdmission;
import io.webrtc.signaling.rpc.RpcTlsContexts;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.DbPools;
import io.webrtc.signaling.storage.HomeParticipationService;
import io.webrtc.signaling.storage.SqlTransactions;
import io.webrtc.signaling.storage.worker.PrivacyDeletionWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericApplicationContext;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Registers the actor-plane native enrollments of one LOCAL_TEST_ONLY container. Values that own no
 * resource (configuration, TLS contexts, keys, the directory snapshot) are built and validated here,
 * so every defect fails startup with a {@link LocalRuntimeFailure} before any bean exists. Owners
 * of threads, sockets, pools or the ActorSystem are created by bean suppliers, so the context owns
 * them; the ActorSystem itself is created only by {@code NativeActorProcessConfiguration}.
 *
 * <p>Every bean is a definition from {@code registerBean(Class, Supplier)}, never a registered
 * singleton, so {@code @ConditionalOnBean} sees it. The actor opens only its own cell database;
 * the directory credentials in the mounted contract are never read.
 */
public final class LocalActorInstaller {
    static final RpcAdmission ADMISSION = new RpcAdmission(8, 1048576, 8, 1048576);
    /** Calls whose relay rounds one actor caches; NativeRelayProducer allows 1..4096. */
    static final int RELAY_CAPACITY = 64;
    static final long RELAY_MEMORY_BYTES = 4L * 1024 * 1024;
    private static final int PRIVACY_KEY_MIN = 32;
    private static final int PRIVACY_KEY_MAX = 64;
    /** The remoting store passwords, read back from the built Pekko config (never logged). */
    private static final String REMOTING_TLS = "pekko.remote.artery.ssl.config-ssl-engine.";
    private static final String KEYSTORE_PASSWORD = REMOTING_TLS + "key-store-password";
    private static final String KEY_PASSWORD = REMOTING_TLS + "key-password";
    private static final String TRUSTSTORE_PASSWORD = REMOTING_TLS + "trust-store-password";
    private static final Logger LOG = LoggerFactory.getLogger(LocalActorInstaller.class);

    /**
     * No authenticated promotion or migration approval source exists for local containers: every
     * epoch adoption is refused, so call participation stays on the enrolled epochs.
     */
    static final HomeParticipationService.EpochAdoptionVerifier NO_EPOCH_ADOPTION =
            (coordinatorCell, previousEpoch, nextEpoch) -> false;

    /**
     * There is no production {@link PrivacyDeletionWorker.Detachment} yet. This one refuses every
     * detachment, so the worker never marks a deletion request complete; requests stay pending
     * rather than being reported as done.
     */
    static final PrivacyDeletionWorker.Detachment UNENROLLED_DETACHMENT =
            opaqueSubject ->
                    new DbOperation<>(
                            CompletableFuture.failedFuture(
                                    new IllegalStateException("Privacy detachment not enrolled")),
                            CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));

    private LocalActorInstaller() {
    }

    public static void install(GenericApplicationContext ctx, LocalRuntimeContract c) {
        install(ctx, c, System::getenv, LocalDatabases::open);
    }

    /**
     * As {@link #install(GenericApplicationContext, LocalRuntimeContract)} with the OS environment
     * (for the Kubernetes API address check) and the database opener injected for tests.
     */
    static void install(
            GenericApplicationContext ctx,
            LocalRuntimeContract c,
            Function<String, String> processEnvironment,
            Function<NativeControlDatabaseEnrollment.Database, SqlTransactions> opener) {
        var profile = c.profile();
        if (profile.plane() != Plane.ACTOR) throw profileInvalid();
        c.requireIdentity(ctx.getEnvironment());
        var contract = c.contract();

        var config = LocalPekkoConfig.build(c, ctx.getEnvironment(), processEnvironment);
        var management =
                LocalTls.jdkContext(
                        c,
                        config.getString(KEYSTORE_PASSWORD).toCharArray(),
                        config.getString(KEY_PASSWORD).toCharArray(),
                        config.getString(TRUSTSTORE_PASSWORD).toCharArray());
        var process = LocalPekkoConfig.process(c, config, management, management);
        var fingerprint = config.getString("signaling.cluster-fingerprint");
        var snapshot = LocalDirectorySnapshot.load(c);
        var wal = wal(c);
        var source = source(c);
        var proofs = LocalTrust.homeProof(c);
        // Keys are parsed and checked now; only the thread-owning wrapper is built lazily.
        var jwt = LocalTrust.jwtVerifier(c);
        var maintenance = new NativeActorMaintenanceEnrollment(privacyKey(c), UNENROLLED_DETACHMENT);
        var observation = observation(c, fingerprint, snapshot);
        var cells = cells(c);
        var ingressTls = LocalTls.actorRpcServer(c);
        var relayTls = LocalTls.relayClients(c);
        var health = new InetSocketAddress(profile.podIp(), profile.healthPort());
        var workloads = workloads(contract);
        var relayDestinations = relayDestinations(snapshot);

        ctx.registerBean(NativeActorProcessEnrollment.class, () -> process);
        ctx.registerBean(NativeActorSourceEnrollment.class, () -> source);
        ctx.registerBean(NativeWalEnrollment.class, () -> wal);
        ctx.registerBean(NativeActorMaintenanceEnrollment.class, () -> maintenance);
        ctx.registerBean(NativeCompatibilityObservationEnrollment.class, () -> observation);
        ctx.registerBean(NativeCellRpcEnrollment.class, () -> cells);
        // Replaces the composition's @ConditionalOnMissingBean default.
        ctx.registerBean(ClusterReadiness.class, () -> ClusterReadiness.localMinikube("LOCAL_TEST_ONLY"));

        // Only the cell database: the directory credentials are never opened by an actor.
        ctx.registerBean(SqlTransactions.class, () -> opener.apply(contract.cellDatabase()));
        // NativeActorRuntimeHooks drains the boundary and then closes the pools.
        ctx.registerBean(
                DbBoundary.class,
                () -> ctx.getBean(SqlTransactions.class).boundary(),
                definition -> definition.setDestroyMethodName(""));
        ctx.registerBean(
                DbPools.class,
                () -> ctx.getBean(SqlTransactions.class).pools(),
                definition -> definition.setDestroyMethodName(""));
        // The verifier owns a worker pool; the caller owns it and the context closes it.
        ctx.registerBean(
                BoundedTokenVerifier.class,
                () -> LocalTrust.bounded(jwt),
                definition -> definition.setDestroyMethodName("close"));

        // NativeActorProcess creates its ActorSystem in its constructor and depends only on the
        // process enrollment, so these lazy lookups cannot form a cycle and never wait for Up.
        ctx.registerBean(
                NativeActorBusinessEnrollment.class,
                () ->
                        new NativeActorBusinessEnrollment(
                                ctx.getBean(NativeActorProcess.class).system(),
                                contract.cell(),
                                contract.storageEpoch(),
                                contract.routingEpoch(),
                                profile.podUid(),
                                proofs,
                                snapshot::home,
                                NO_EPOCH_ADOPTION,
                                ctx.getBean(BoundedTokenVerifier.class)));
        ctx.registerBean(
                NativeActorSchedulingEnrollment.class,
                () -> {
                    var system = ctx.getBean(NativeActorProcess.class).system();
                    return new NativeActorSchedulingEnrollment(
                            system,
                            Set.of(profile.azRole()),
                            fingerprint,
                            List.of(),
                            LocalActorInstaller::workerEvent,
                            health,
                            () -> !system.getWhenTerminated().toCompletableFuture().isDone(),
                            () -> ctx.getBean(NativeTelemetry.class).scrape());
                });
        ctx.registerBean(
                NativeActorIngressEnrollment.class,
                () ->
                        new NativeActorIngressEnrollment(
                                contract.environment(),
                                profile.businessPort(),
                                ingressTls,
                                ADMISSION,
                                NativeSagaEffects.Network.from(ctx.getBean(CellRpcClient.class)),
                                RELAY_CAPACITY,
                                new RelayBufferBudget(RELAY_MEMORY_BYTES),
                                new GatewayRelayRpcClient(
                                        contract.environment(),
                                        relayDestinations.size(),
                                        relayDestinations,
                                        relayTls,
                                        ADMISSION),
                                workloads));
    }

    /** Each enrolled gateway at its pinned boot, addressed as the mounted contract says. */
    static Map<GatewayRelayRpcClient.Target, CellRpcClient.Endpoint> relayDestinations(
            LocalDirectorySnapshot snapshot) {
        var destinations = new HashMap<GatewayRelayRpcClient.Target, CellRpcClient.Endpoint>();
        snapshot.gateways()
                .forEach(
                        (target, gateway) ->
                                destinations.put(
                                        target,
                                        new CellRpcClient.Endpoint(
                                                gateway.host(), gateway.port(), gateway.tlsAuthority())));
        return Map.copyOf(destinations);
    }

    /** A standby-less writer is not a local mode: at least one synchronous replica is required. */
    static NativeWalEnrollment wal(LocalRuntimeContract c) {
        var wal = c.profile().wal();
        if (wal == null || wal.replicas().isEmpty() || wal.synchronousReplicas() < 1)
            throw profileInvalid();
        try {
            return new NativeWalEnrollment(
                    c.contract().cell(),
                    c.contract().storageEpoch(),
                    wal.systemIdentifier(),
                    wal.timeline(),
                    wal.replicas(),
                    wal.synchronousReplicas(),
                    wal.maximumLagBytes(),
                    wal.freshness(),
                    wal.maximumReplyAge(),
                    false);
        } catch (IllegalArgumentException rejected) {
            throw profileInvalid();
        }
    }

    private static NativeActorSourceEnrollment source(LocalRuntimeContract c) {
        var contract = c.contract();
        var endpoints = LocalTrust.sourceEndpoints(c);
        try {
            return new NativeActorSourceEnrollment(
                    contract.cell(),
                    contract.storageEpoch(),
                    c.profile().podUid(),
                    c.processBoot(),
                    contract.identity(),
                    endpoints.clock(),
                    endpoints.revocations());
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /**
     * Raw key bytes, 32 to 64 of them, from the confined file the profile names. The file itself
     * fails like every mounted file ({@code PATH/MISSING} when absent or unreadable, {@code
     * PATH/PATH_ESCAPE} outside the mount); any defect of the key, whether the field is absent or
     * the content is empty, too short or too long, is {@code PROFILE/INVALID}.
     */
    static byte[] privacyKey(LocalRuntimeContract c) {
        var path = c.profile().privacyPseudonymKey();
        if (path == null) throw profileInvalid();
        byte[] key;
        try {
            key = LocalFiles.read(c.resolve(path), PRIVACY_KEY_MAX);
        } catch (LocalRuntimeFailure failure) {
            // LocalFiles reports content size as a CONTRACT failure; for this key it is the profile's.
            if (failure.role() == Role.CONTRACT) throw profileInvalid();
            throw failure;
        }
        if (key.length < PRIVACY_KEY_MIN) throw profileInvalid();
        return key;
    }

    /** Shared with the gateway installer: the same topology and build pin on every plane. */
    static NativeCompatibilityObservationEnrollment observation(
            LocalRuntimeContract c, String fingerprint, LocalDirectorySnapshot snapshot) {
        var build = c.profile().compatibility();
        if (build == null) throw profileInvalid();
        try {
            var features = EnumSet.noneOf(CompatibilityWindow.Feature.class);
            for (var name : build.features())
                features.add(
                        CompatibilityWindow.Feature.valueOf(name.replace('-', '_').toUpperCase(Locale.ROOT)));
            return new NativeCompatibilityObservationEnrollment(
                    c.contract().cell(),
                    new NativeCompatibilityEnrollment.BuildIdentity(
                            build.major(), build.minor(), build.executableId(), fingerprint, features),
                    new NativeCompatibilityEnrollment.ApprovedWindow(
                            build.major(), build.lowerMinor(), build.upperMinor()),
                    c.contract().cells().stream()
                            .map(MountedNativeContract.CellTarget::cell)
                            .collect(Collectors.toUnmodifiableSet()),
                    snapshot.targets());
        } catch (IllegalArgumentException rejected) {
            throw profileInvalid();
        }
    }

    /** Shared with the gateway installer: every contract cell, pinned to its actor identity. */
    static NativeCellRpcEnrollment cells(LocalRuntimeContract c) {
        return cells(c, LocalTls.rpcClients(c));
    }

    /** As {@link #cells(LocalRuntimeContract)}, presenting the given client identity. */
    static NativeCellRpcEnrollment cells(LocalRuntimeContract c, RpcTlsContexts.ClientTls tls) {
        var destinations = new HashMap<String, CellRpcClient.Endpoint>();
        for (var cell : c.contract().cells())
            destinations.put(
                    cell.cell(), new CellRpcClient.Endpoint(cell.host(), cell.port(), cell.tlsAuthority()));
        try {
            return new NativeCellRpcEnrollment(c.contract().environment(), destinations, tls, ADMISSION);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /**
     * A gateway peer is admitted only for itself, in its own cell, and only when the mounted
     * contract enrolls that gateway. A missing peer or identity is refused.
     */
    static BiPredicate<CellRpcServer.Peer, NativeSessionHandler.GatewayIdentity> workloads(
            MountedNativeContract contract) {
        var enrolled =
                contract.gateways().stream()
                        .map(g -> g.cell() + "/" + g.gatewayId())
                        .collect(Collectors.toUnmodifiableSet());
        return (peer, gateway) ->
                peer != null
                        && gateway != null
                        && "gateway".equals(peer.role())
                        && peer.cell().equals(gateway.cell())
                        && peer.workloadId().equals(gateway.gatewayId())
                        && enrolled.contains(gateway.cell() + "/" + gateway.gatewayId());
    }

    /** Worker outcomes other than completion are reported by name and status only. */
    static void workerEvent(NativeWorkerScheduler.Event event) {
        if (event.status() != NativeWorkerScheduler.Status.COMPLETED)
            LOG.warn("Native worker {} ended {}", event.name(), event.status());
    }

    private static LocalRuntimeFailure profileInvalid() {
        return LocalJson.failure(Role.PROFILE, Category.INVALID);
    }
}
