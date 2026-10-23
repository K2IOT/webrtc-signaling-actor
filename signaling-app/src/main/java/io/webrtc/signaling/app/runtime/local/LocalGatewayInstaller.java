package io.webrtc.signaling.app.runtime.local;

import io.netty.handler.codec.http.HttpHeaders;
import io.webrtc.signaling.actors.cluster.ClusterShape;
import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeCachedRevocationSource;
import io.webrtc.signaling.app.runtime.NativeCachedSecurity;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeClockSource;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeGatewayLifecycleEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.EdgeAdmission;
import io.webrtc.signaling.gateway.GatewaySecuritySweep;
import io.webrtc.signaling.gateway.GatewayServer;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.NativeSessionHandler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Registers the gateway-plane native enrollments of one LOCAL_TEST_ONLY container, plus the
 * trusted-clock and scoped-security owners that no auto-configuration provides for this plane.
 * Values that own no resource (keys, the directory snapshot, TLS contexts, the clock monitor and the
 * scoped security cache) are built and validated first, so every defect fails startup with a
 * {@link LocalRuntimeFailure} before any definition exists. The gateway opens no database.
 *
 * <p>The clock and revocation sources and their worker are plain definitions here rather than a new
 * auto-configuration: {@code NativeGatewayLifecycleConfiguration} already takes them through
 * {@code ObjectProvider}, starts the worker and drains all three, and {@code NativeStartupCleanup}
 * drains them when startup fails. Bean destruction therefore never retires them.
 */
public final class LocalGatewayInstaller {
    /** Origin-less (native) upgrades are refused; only the profile's browser origins are admitted. */
    static final Predicate<HttpHeaders> NO_NATIVE_CLIENTS = headers -> false;
    /** Listeners bind every interface; the private health probe never does. */
    static final String ALL_INTERFACES = "0.0.0.0";
    /**
     * Local ingress bounds (Ruling 20). The enrollment allows 1..64 event loops, 1..1000
     * unauthenticated and up to 200000 connections.
     */
    static final int EVENT_LOOPS = 2;
    static final int MAX_UNAUTHENTICATED = 64;
    static final int MAX_CONNECTIONS = 1024;
    /** Relay proof cache bounds (Ruling 20); the enrollment allows 1..4096 and 1..64. */
    static final int RELAY_CACHE_CAPACITY = 1024;
    static final int RELAY_PENDING_LIMIT = 32;
    /** Scoped revocation facts retained before re-enrollment is required (cache allows 1..65536). */
    static final int SECURITY_CAPACITY = 4096;
    /** Both safety sources poll ten times per second, as on the control plane. */
    static final Duration SOURCE_PERIOD = Duration.ofMillis(100);
    /**
     * Liveness only: an answering probe proves the process is alive. Readiness is derived by {@code
     * NativeGatewaySpringLifecycle} from boot, compatibility, ingress and safety.
     */
    static final BooleanSupplier PROCESS_LIVE = () -> true;

    private LocalGatewayInstaller() {
    }

    public static void install(GenericApplicationContext ctx, LocalRuntimeContract c) {
        install(ctx, c, Clock.systemUTC(), System::nanoTime);
    }

    /**
     * As {@link #install(GenericApplicationContext, LocalRuntimeContract)} with the scoped security
     * cache's wall and monotonic clocks injected for tests. The revocation source stamps each request
     * with {@link System#nanoTime()}, so production must pass exactly that.
     */
    static void install(
            GenericApplicationContext ctx, LocalRuntimeContract c, Clock wall, LongSupplier elapsed) {
        var profile = c.profile();
        // Transfer cells never reach here: LocalProcessProfile refuses them on every non-control plane.
        if (profile.plane() != Plane.GATEWAY
                || profile.gatewayId() == null
                || profile.allowedOrigins().isEmpty()
                || profile.relayPort() == profile.businessPort()
                || profile.relayPort() == profile.healthPort()) throw profileInvalid();
        c.requireIdentity(ctx.getEnvironment());
        var shape = LocalPekkoConfig.shape(ctx.getEnvironment());
        var contract = c.contract();

        var fingerprint = LocalPekkoConfig.fingerprint(ctx.getEnvironment());
        var snapshot = LocalDirectorySnapshot.load(c);
        var identity = identity(c, snapshot);
        var health = healthAddress(profile);
        var ingress = ingress(c);
        var cells = LocalActorInstaller.cells(c);
        var observation = LocalActorInstaller.observation(c, fingerprint, snapshot);
        // Verify-only (Ruling 19): the internet-facing gateway never opens the proof signing key.
        var relayProofs = LocalTrust.relayVerifier(c);
        // Keys are parsed and checked now; only the thread-owning wrapper is built lazily.
        var jwt = LocalTrust.jwtVerifier(c);
        var clock = LocalTrust.clockMonitor(c);
        var sources = LocalTrust.sourceEndpoints(c);
        var security = security(c, sources.revocations(), wall, elapsed);

        // The size every cell is expected to have: read by the compatibility gate and collectors.
        ctx.registerBean(ClusterShape.class, () -> shape);
        ctx.registerBean(NativeGatewayIngressEnrollment.class, () -> ingress);
        ctx.registerBean(NativeCellRpcEnrollment.class, () -> cells);
        ctx.registerBean(NativeCompatibilityObservationEnrollment.class, () -> observation);
        ctx.registerBean(ClockSafetyMonitor.class, () -> clock);
        ctx.registerBean(NativeCachedSecurity.class, () -> security);
        // The verifier owns a worker pool; the caller owns it and the context closes it.
        ctx.registerBean(
                BoundedTokenVerifier.class,
                () -> LocalTrust.bounded(jwt),
                definition -> definition.setDestroyMethodName("close"));

        Consumer<BeanDefinition> drainedByLifecycle = definition -> definition.setDestroyMethodName("");
        ctx.registerBean(
                NativeClockSource.class,
                () ->
                        new NativeClockSource(
                                sources.clock().uri(),
                                sources.clock().tls(),
                                ctx.getBean(ClockSafetyMonitor.class),
                                profile.podUid(),
                                c.processBoot()),
                drainedByLifecycle::accept);
        ctx.registerBean(
                NativeCachedRevocationSource.class,
                () ->
                        new NativeCachedRevocationSource(
                                sources.revocations().uri(),
                                sources.revocations().tls(),
                                ctx.getBean(NativeCachedSecurity.class),
                                profile.podUid(),
                                c.processBoot()),
                drainedByLifecycle::accept);
        ctx.registerBean(
                NativeWorkerScheduler.class,
                () ->
                        new NativeWorkerScheduler(
                                List.of(
                                        ctx.getBean(NativeClockSource.class).job(SOURCE_PERIOD),
                                        ctx.getBean(NativeCachedRevocationSource.class).job(SOURCE_PERIOD)),
                                LocalActorInstaller::workerEvent)
                                .observe(ctx.getBean(NativeTelemetry.class)::workerStarted),
                drainedByLifecycle::accept);

        ctx.registerBean(
                NativeGatewayBusinessEnrollment.class,
                () -> {
                    var cached = ctx.getBean(NativeCachedSecurity.class);
                    var revocations = ctx.getBean(NativeCachedRevocationSource.class);
                    return new NativeGatewayBusinessEnrollment(
                            identity,
                            contract.routingEpoch(),
                            ctx.getBean(BoundedTokenVerifier.class),
                            cached::check,
                            // Fresh only while the source is not draining and its last page is in bound.
                            revocations::usable,
                            snapshot::home,
                            relayProofs,
                            RELAY_CACHE_CAPACITY,
                            RELAY_PENDING_LIMIT);
                });
        ctx.registerBean(
                NativeGatewayLifecycleEnrollment.class,
                () ->
                        new NativeGatewayLifecycleEnrollment(
                                health, PROCESS_LIVE, () -> ctx.getBean(NativeTelemetry.class).scrape()));
    }

    /**
     * This gateway at the boot the profile pins. The directory snapshot must pin the same boot for
     * this gateway in this cell, so the actors' relay targets name exactly this process's boot.
     * There is no region field in the contract: the cell id stands in for it.
     */
    static NativeSessionHandler.GatewayIdentity identity(
            LocalRuntimeContract c, LocalDirectorySnapshot snapshot) {
        var profile = c.profile();
        var contract = c.contract();
        GatewayRelayRpcClient.Target own;
        NativeSessionHandler.GatewayIdentity identity;
        try {
            own = new GatewayRelayRpcClient.Target(contract.cell(), profile.gatewayId(), profile.gatewayBootId());
            identity =
                    new NativeSessionHandler.GatewayIdentity(
                            profile.gatewayId(),
                            profile.gatewayBootId(),
                            contract.cell(),
                            contract.storageEpoch(),
                            contract.cell());
        } catch (IllegalArgumentException | NullPointerException rejected) {
            throw profileInvalid();
        }
        if (!snapshot.targets().contains(own))
            throw LocalJson.failure(Role.IDENTITY, Category.IDENTITY_MISMATCH);
        return identity;
    }

    /** A concrete, resolved, non-wildcard address: what the private health server must bind. */
    static boolean concrete(InetSocketAddress address) {
        return address != null && !address.isUnresolved() && !address.getAddress().isAnyLocalAddress();
    }

    /** Shared with the control installer: the private probe on the pod address. */
    static InetSocketAddress healthAddress(LocalProcessProfile profile) {
        // podIp is a literal dotted quad (profile validation), so this performs no name lookup.
        var address = new InetSocketAddress(profile.podIp(), profile.healthPort());
        if (!concrete(address)) throw profileInvalid();
        return address;
    }

    private static NativeGatewayIngressEnrollment ingress(LocalRuntimeContract c) {
        var profile = c.profile();
        var wssTls = LocalTls.wssServer(c);
        var rpcTls = LocalTls.gatewayRpcServer(c);
        try {
            return new NativeGatewayIngressEnrollment(
                    new InetSocketAddress(ALL_INTERFACES, profile.businessPort()),
                    wssTls,
                    // GatewayServer itself requires GET /ws exactly and the webrtc-signaling.v1 subprotocol.
                    new GatewayServer.UpgradePolicy(new HashSet<>(profile.allowedOrigins()), NO_NATIVE_CLIENTS),
                    EVENT_LOOPS,
                    MAX_UNAUTHENTICATED,
                    MAX_CONNECTIONS,
                    EdgeAdmission.Limits.candidate(),
                    c.contract().environment(),
                    profile.relayPort(),
                    rpcTls,
                    LocalActorInstaller.ADMISSION,
                    LocalActorInstaller.ADMISSION,
                    GatewaySecuritySweep.Settings.candidate());
        } catch (IllegalArgumentException rejected) {
            throw profileInvalid();
        }
    }

    /**
     * Admission facts verified against the revocation source's own keys; starts closed. Shared with
     * the control installer.
     */
    static NativeCachedSecurity security(
            LocalRuntimeContract c,
            NativeActorSourceEnrollment.Endpoint revocations,
            Clock wall,
            LongSupplier elapsed) {
        var identity = c.contract().identity();
        try {
            return new NativeCachedSecurity(
                    identity,
                    new RevocationSourceVerifier(c.contract().cell(), identity.issuer(), revocations.keys()),
                    SECURITY_CAPACITY,
                    wall,
                    elapsed);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    private static LocalRuntimeFailure profileInvalid() {
        return LocalJson.failure(Role.PROFILE, Category.INVALID);
    }
}
