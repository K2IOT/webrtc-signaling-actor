package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.actors.cluster.ClusterShape;
import io.webrtc.signaling.app.SignalingApplication.Plane;
import io.webrtc.signaling.app.runtime.NativeActorSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeBucketTransferEnrollment;
import io.webrtc.signaling.app.runtime.NativeCachedRevocationSource;
import io.webrtc.signaling.app.runtime.NativeCachedSecurity;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.app.runtime.NativeCompatibilityObservationEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlBusinessEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlDatabaseResources;
import io.webrtc.signaling.app.runtime.NativeControlIngressEnrollment;
import io.webrtc.signaling.app.runtime.NativeControlSourceEnrollment;
import io.webrtc.signaling.app.runtime.NativeTelemetry;
import io.webrtc.signaling.app.runtime.NativeWorkerScheduler;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.control.PostgresBucketTransfer;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import io.webrtc.signaling.storage.SqlTransactions;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Registers the control-plane native enrollments of one LOCAL_TEST_ONLY container. Values that own
 * no resource (keys, the directory snapshot, TLS contexts, the scoped security cache, the database
 * enrollments) are built and validated first, so every defect fails startup with a {@link
 * LocalRuntimeFailure} before any definition exists.
 *
 * <p>The existing auto-configurations own the rest: {@code NativeControlDatabaseConfiguration}
 * opens the regional directory and this cell's database and builds the directory repository; {@code
 * NativeControlSourceConfiguration} builds the clock monitor, its source and worker; {@code
 * NativeCellRpcConfiguration} builds the compatibility-observation client; {@code
 * NativeCompatibilityObservationConfiguration} derives the compatibility gate from real
 * observations. Business suppliers look those owners up lazily, so the business clock is the very
 * monitor the clock source feeds.
 *
 * <p>LOCAL_TEST_ONLY: control is the only plane that holds more than one cell database credential.
 * Besides its own cell it opens exactly the one transfer cell of the profile, so the direct bucket
 * transfer can run. Remote transfer (A3/A4) removes the second credential.
 */
public final class LocalControlInstaller {
    /**
     * Regional directory writer admission, checked before and after every regional statement. No
     * fencing owner (lease or authority epoch) exists for it yet; compose runs exactly one control
     * container, which is therefore the sole regional writer. Deferred: a real fence arrives with
     * A3/A4. This is the only unconditional predicate the control plane is given.
     */
    static final BooleanSupplier SOLE_REGIONAL_WRITER = () -> true;
    /** The largest refresh the business enrollment allows. */
    static final Duration DIRECTORY_REFRESH = Duration.ofSeconds(30);
    /** The transfer cell's database, opened here; the control process drains it. */
    static final String TRANSFER_TARGET_SQL = "localControlTransferTargetSql";
    /**
     * Liveness only. Readiness is {@code NativeControlReadiness}: compatibility, transfer, clock and
     * security freshness.
     */
    static final BooleanSupplier PROCESS_LIVE = () -> true;

    private LocalControlInstaller() {
    }

    public static void install(GenericApplicationContext ctx, LocalRuntimeContract c) {
        install(ctx, c, Clock.systemUTC(), System::nanoTime, LocalDatabases::open);
    }

    /**
     * As {@link #install(GenericApplicationContext, LocalRuntimeContract)} with the scoped security
     * cache's clocks and the transfer database opener injected for tests. The revocation source
     * stamps requests with {@link System#nanoTime()}, so production must pass exactly that.
     */
    static void install(
            GenericApplicationContext ctx,
            LocalRuntimeContract c,
            Clock wall,
            LongSupplier elapsed,
            Function<NativeControlDatabaseEnrollment.Database, SqlTransactions> opener) {
        var profile = c.profile();
        if (profile.plane() != Plane.CONTROL) throw profileInvalid();
        var transfer = transferCell(c);
        c.requireIdentity(ctx.getEnvironment());
        var shape = LocalPekkoConfig.shape(ctx.getEnvironment());
        var contract = c.contract();

        var fingerprint = LocalPekkoConfig.fingerprint(ctx.getEnvironment());
        var snapshot = LocalDirectorySnapshot.load(c);
        var observation = LocalActorInstaller.observation(c, fingerprint, snapshot);
        // The control identity: actor servers admit it for compatibility observation only.
        var cells = LocalActorInstaller.cells(c, LocalTls.controlRpcClients(c));
        var ingress = ingress(c, ctx);
        // Keys are parsed and checked now; only the thread-owning wrapper is built lazily.
        var jwt = LocalTrust.jwtVerifier(c);
        var sources = LocalTrust.sourceEndpoints(c);
        var security = LocalGatewayInstaller.security(c, sources.revocations(), wall, elapsed);
        var source = source(c, sources.clock());
        var databases = databases(c);

        // The size every cell is expected to have: read by the compatibility gate and collectors.
        ctx.registerBean(ClusterShape.class, () -> shape);
        ctx.registerBean(NativeControlDatabaseEnrollment.class, () -> databases);
        ctx.registerBean(NativeControlSourceEnrollment.class, () -> source);
        ctx.registerBean(NativeControlIngressEnrollment.class, () -> ingress);
        ctx.registerBean(NativeCompatibilityObservationEnrollment.class, () -> observation);
        ctx.registerBean(NativeCellRpcEnrollment.class, () -> cells);
        ctx.registerBean(NativeCachedSecurity.class, () -> security);
        // The verifier owns a worker pool; the caller owns it and the context closes it.
        ctx.registerBean(
                BoundedTokenVerifier.class,
                () -> LocalTrust.bounded(jwt),
                definition -> definition.setDestroyMethodName("close"));

        // NativeControlProcess starts the worker and drains it, the source and the transfer SQL;
        // NativeStartupCleanup drains them when startup fails. Bean destruction never retires them.
        Consumer<BeanDefinition> drainedByProcess = definition -> definition.setDestroyMethodName("");
        ctx.registerBean(
                NativeCachedRevocationSource.class,
                () ->
                        new NativeCachedRevocationSource(
                                sources.revocations().uri(),
                                sources.revocations().tls(),
                                ctx.getBean(NativeCachedSecurity.class),
                                profile.podUid(),
                                c.processBoot()),
                drainedByProcess::accept);
        ctx.registerBean(
                NativeWorkerScheduler.class,
                () ->
                        new NativeWorkerScheduler(
                                List.of(
                                        ctx.getBean(NativeCachedRevocationSource.class)
                                                .job(LocalGatewayInstaller.SOURCE_PERIOD)),
                                LocalActorInstaller::workerEvent)
                                .observe(ctx.getBean(NativeTelemetry.class)::workerStarted),
                drainedByProcess::accept);
        ctx.registerBean(
                TRANSFER_TARGET_SQL,
                SqlTransactions.class,
                () -> opener.apply(transfer.database()),
                drainedByProcess::accept);
        ctx.registerBean(
                NativeBucketTransferEnrollment.class,
                () ->
                        new NativeBucketTransferEnrollment(
                                // This cell's SQL owner, the one the control database configuration opened.
                                new PostgresBucketTransfer.Store(
                                        ctx.getBean(NativeControlDatabaseResources.class).local(),
                                        contract.cell(),
                                        contract.storageEpoch()),
                                new PostgresBucketTransfer.Store(
                                        ctx.getBean(TRANSFER_TARGET_SQL, SqlTransactions.class),
                                        transfer.cell(),
                                        transfer.storageEpoch())));
        ctx.registerBean(
                NativeControlBusinessEnrollment.class,
                () -> {
                    var cached = ctx.getBean(NativeCachedSecurity.class);
                    var revocations = ctx.getBean(NativeCachedRevocationSource.class);
                    return new NativeControlBusinessEnrollment(
                            ctx.getBean(PostgresDirectoryRepository.class),
                            ctx.getBean(BoundedTokenVerifier.class),
                            cached::check,
                            // Fresh only while the source is not draining and its last page is in bound.
                            revocations::usable,
                            // The monitor NativeControlSourceConfiguration feeds from the clock source.
                            ctx.getBean(ClockSafetyMonitor.class),
                            DIRECTORY_REFRESH);
                });
    }

    /**
     * The profile's one transfer cell: another cell the contract enrolls at the same storage epoch,
     * with a database of its own. Zero, several, this cell, an unenrolled cell or a shared database
     * is {@code PROFILE/INVALID}.
     */
    static LocalProcessProfile.TransferCell transferCell(LocalRuntimeContract c) {
        var cells = c.profile().transferCells();
        if (cells.size() != 1) throw profileInvalid();
        var transfer = cells.getFirst();
        var contract = c.contract();
        var url = transfer.database().jdbcUrl();
        if (transfer.cell().equals(contract.cell())
                || contract.cells().stream()
                .noneMatch(
                        t -> t.cell().equals(transfer.cell()) && t.storageEpoch() == transfer.storageEpoch())
                || url.equals(contract.cellDatabase().jdbcUrl())
                || url.equals(contract.directoryDatabase().jdbcUrl())) throw profileInvalid();
        return transfer;
    }

    private static NativeControlDatabaseEnrollment databases(LocalRuntimeContract c) {
        var contract = c.contract();
        try {
            return new NativeControlDatabaseEnrollment(
                    contract.directoryDatabase(),
                    contract.cellDatabase(),
                    contract.cell(),
                    contract.storageEpoch(),
                    SOLE_REGIONAL_WRITER);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    private static NativeControlSourceEnrollment source(
            LocalRuntimeContract c, NativeActorSourceEnrollment.Endpoint clock) {
        try {
            return new NativeControlSourceEnrollment(
                    c.contract().cell(),
                    c.contract().storageEpoch(),
                    c.profile().podUid(),
                    c.processBoot(),
                    clock,
                    LocalActorInstaller::workerEvent);
        } catch (IllegalArgumentException rejected) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Public HTTPS bootstrap on every interface; the private probe on the pod address. */
    private static NativeControlIngressEnrollment ingress(
            LocalRuntimeContract c, GenericApplicationContext ctx) {
        var profile = c.profile();
        var health = LocalGatewayInstaller.healthAddress(profile);
        var tls = LocalTls.wssServer(c);
        try {
            return new NativeControlIngressEnrollment(
                    new InetSocketAddress(LocalGatewayInstaller.ALL_INTERFACES, profile.businessPort()),
                    tls,
                    health,
                    PROCESS_LIVE,
                    () -> ctx.getBean(NativeTelemetry.class).scrape());
        } catch (IllegalArgumentException rejected) {
            throw profileInvalid();
        }
    }

    private static LocalRuntimeFailure profileInvalid() {
        return LocalJson.failure(Role.PROFILE, Category.INVALID);
    }
}
