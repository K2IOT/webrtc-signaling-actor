package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ClusterShape;
import io.webrtc.signaling.app.CompatibilityWindow;
import io.webrtc.signaling.protocol.internal.CompatibilityActorDescriptor;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.RpcOperation;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Fixed enrolled fanout. Every original RPC remains owned until its physical terminal callback.
 */
public final class NativeCompatibilityCollectors {
    private final NativeCompatibilityObservationEnrollment inputs;
    private final Supplier<CellRpcClient> cells;
    private final Supplier<GatewayRelayRpcClient> gateways;
    private final Map<String, NativeCompatibilityEnrollment.ActorPeers> actors =
            new ConcurrentHashMap<>();
    private final Map<
            NativeCompatibilityEnrollment.GatewayBoot, NativeCompatibilityEnrollment.GatewayPeer>
            boots = new ConcurrentHashMap<>();
    private final Map<String, NativeCompatibilityEnrollment.SchemaObservation> schemas =
            new ConcurrentHashMap<>();
    private final Supplier<NativeCompatibilityEnrollment.RollbackObservation> rollback;
    private final ClusterShape shape;
    private volatile Supplier<CompatibilityObservationReply> localGateway = () -> null;

    public NativeCompatibilityCollectors(
            NativeCompatibilityObservationEnrollment inputs,
            Supplier<CellRpcClient> cells,
            Supplier<GatewayRelayRpcClient> gateways,
            Supplier<NativeCompatibilityEnrollment.RollbackObservation> rollback) {
        this(inputs, cells, gateways, rollback, ClusterShape.PRODUCTION);
    }

    /** As above; {@code shape} is the size every observed cell is expected to have. */
    public NativeCompatibilityCollectors(
            NativeCompatibilityObservationEnrollment inputs,
            Supplier<CellRpcClient> cells,
            Supplier<GatewayRelayRpcClient> gateways,
            Supplier<NativeCompatibilityEnrollment.RollbackObservation> rollback,
            ClusterShape shape) {
        this.shape = Objects.requireNonNull(shape);
        this.inputs = inputs;
        this.cells = cells;
        this.gateways = gateways;
        this.rollback = rollback;
    }

    private static Set<CompatibilityWindow.Feature> features(CompatibilityActorDescriptor actor) {
        var result = EnumSet.noneOf(CompatibilityWindow.Feature.class);
        for (var feature : actor.getFeaturesList())
            if (!result.add(CompatibilityWindow.Feature.valueOf(feature)))
                throw new IllegalStateException("Duplicate feature");
        return result;
    }

    public void localGateway(Supplier<CompatibilityObservationReply> source) {
        localGateway = Objects.requireNonNull(source);
    }

    public NativeCompatibilityEnrollment enrollment() {
        return new NativeCompatibilityEnrollment(
                inputs.installedBuild(),
                inputs.approvedWindow(),
                () -> Map.copyOf(actors),
                () -> Map.copyOf(boots),
                () -> Map.copyOf(schemas),
                rollback);
    }

    public NativeWorkerScheduler.Job pollJob() {
        return new NativeWorkerScheduler.Job(
                "compatibility_poll",
                NativeWorkerScheduler.Priority.SAFETY,
                Duration.ofSeconds(1),
                this::poll);
    }

    private RpcOperation<?> poll(Duration budget) {
        long end = System.nanoTime() + budget.toNanos();
        var physical = new ArrayList<CompletableFuture<?>>();
        var logical = new ArrayList<CompletableFuture<?>>();
        var client = cells.get();
        var relay = gateways.get();
        for (String cell : inputs.cells()) {
            if (client == null || end - System.nanoTime() <= 0) {
                actors.remove(cell);
                schemas.remove(cell);
                continue;
            }
            try {
                var operation =
                        client.observeCompatibility(cell, Duration.ofNanos(end - System.nanoTime()));
                physical.add(operation.physicalCompletion().toCompletableFuture());
                logical.add(
                        operation
                                .logical()
                                .handle(
                                        (reply, error) -> {
                                            try {
                                                if (error != null || System.nanoTime() >= end)
                                                    throw new IllegalStateException("Observation unavailable");
                                                acceptActors(cell, reply);
                                            } catch (RuntimeException unavailable) {
                                                actors.remove(cell);
                                                schemas.remove(cell);
                                            }
                                            return null;
                                        })
                                .toCompletableFuture());
            } catch (RuntimeException unknown) {
                actors.remove(cell);
                schemas.remove(cell);
                // A throwing dispatch provides no proof that physical work did not start.
                physical.add(new CompletableFuture<>());
            }
        }
        for (var target : inputs.gateways()) {
            var key =
                    new NativeCompatibilityEnrollment.GatewayBoot(
                            target.cell(), target.gatewayId(), target.bootId());
            try {
                var local = localGateway.get();
                if (local != null
                        && local.getSourceCell().equals(target.cell())
                        && local.getGatewayId().equals(target.gatewayId())
                        && local.getBootId().equals(target.bootId().toString())) {
                    acceptGateway(key, local);
                    continue;
                }
            } catch (RuntimeException unavailable) {
                boots.remove(key);
            }
            if (relay == null || end - System.nanoTime() <= 0) {
                boots.remove(key);
                continue;
            }
            try {
                var operation =
                        relay.observeCompatibility(target, Duration.ofNanos(end - System.nanoTime()));
                physical.add(operation.physicalCompletion().toCompletableFuture());
                logical.add(
                        operation
                                .logical()
                                .handle(
                                        (reply, error) -> {
                                            try {
                                                if (error != null
                                                        || System.nanoTime() >= end
                                                        || !reply.hasGateway()
                                                        || reply.getActorsCount() != 0)
                                                    throw new IllegalStateException("Observation unavailable");
                                                acceptGateway(key, reply);
                                            } catch (RuntimeException unavailable) {
                                                boots.remove(key);
                                            }
                                            return null;
                                        })
                                .toCompletableFuture());
            } catch (RuntimeException unknown) {
                boots.remove(key);
                physical.add(new CompletableFuture<>());
            }
        }
        return new RpcOperation<>(
                CompletableFuture.allOf(logical.toArray(CompletableFuture[]::new)),
                CompletableFuture.allOf(physical.toArray(CompletableFuture[]::new)));
    }

    private void acceptGateway(
            NativeCompatibilityEnrollment.GatewayBoot key, CompatibilityObservationReply reply) {
        long now = Instant.now().toEpochMilli();
        if (!reply.hasGateway()
                || reply.getValidUntilMs() <= now
                || reply.getObservedAtMs() > now
                || reply.getObservedAtMs() < now - 5000
                || reply.getValidUntilMs() > reply.getObservedAtMs() + 5000)
            throw new IllegalStateException("Expired gateway descriptor");
        var descriptor = reply.getGateway();
        validate(descriptor, key.cell());
        boots.put(
                key,
                new NativeCompatibilityEnrollment.GatewayPeer(
                        Instant.ofEpochMilli(reply.getValidUntilMs()),
                        descriptor.getMajor(),
                        descriptor.getMinor(),
                        descriptor.getFingerprint(),
                        features(descriptor)));
    }

    void acceptActors(String cell, CompatibilityObservationReply reply) {
        if (reply.hasGateway() || !shape.admitsPeerCount(reply.getActorsCount()))
            throw new IllegalStateException("Incomplete actor observation");
        var peers = new ArrayList<CompatibilityWindow.Peer>();
        var identities = new HashSet<UUID>();
        for (var actor : reply.getActorsList()) {
            validate(actor, cell);
            var id = UUID.fromString(actor.getMemberId());
            if (!identities.add(id) || !actor.getZone().matches("az-[a-z0-9][a-z0-9-]{0,62}"))
                throw new IllegalStateException("Invalid actor membership");
            peers.add(
                    new CompatibilityWindow.Peer(
                            id,
                            actor.getZone(),
                            cell,
                            actor.getMajor(),
                            actor.getMinor(),
                            actor.getFingerprint(),
                            features(actor)));
        }
        if (peers.stream().map(CompatibilityWindow.Peer::zone).distinct().count() < 2)
            throw new IllegalStateException("Incomplete actor zones");
        actors.put(
                cell,
                new NativeCompatibilityEnrollment.ActorPeers(
                        Instant.ofEpochMilli(reply.getValidUntilMs()), peers));
        long until = Math.min(reply.getValidUntilMs(), reply.getSchemaValidUntilMs());
        if (reply.getSchemaKnown() && until > Instant.now().toEpochMilli())
            schemas.put(
                    cell,
                    new NativeCompatibilityEnrollment.SchemaObservation(
                            Instant.ofEpochMilli(until), reply.getExactMigration16Applied()));
        else schemas.remove(cell);
    }

    private void validate(CompatibilityActorDescriptor actor, String cell) {
        if (!actor.getCell().equals(cell)
                || !actor.getFingerprint().equals(inputs.installedBuild().fingerprint())
                || !actor.getExecutableId().matches("[a-f0-9]{64}")
                || actor.getMajor() < 1
                || actor.getMinor() < 0
                || actor.getFeaturesCount() > 32)
            throw new IllegalStateException("Invalid native build descriptor");
    }
}
