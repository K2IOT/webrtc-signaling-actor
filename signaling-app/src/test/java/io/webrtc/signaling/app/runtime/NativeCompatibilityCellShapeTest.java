package io.webrtc.signaling.app.runtime;

import com.typesafe.config.ConfigFactory;
import io.webrtc.signaling.actors.cluster.ClusterShape;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.JoinSeedNodes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One real three-member, two-zone cell. A cell that declares its size of three is compatible at its
 * quorum of two Up peers; the default (production) shape still needs four and refuses the same cell.
 */
class NativeCompatibilityCellShapeTest {
    static final String FINGERPRINT = "a".repeat(64);
    static final String EXECUTABLE = "b".repeat(64);
    static final List<ActorSystem<Void>> nodes = new ArrayList<>();
    static final List<String> zones = List.of("a", "a", "b");

    @BeforeAll
    static void formCluster() throws Exception {
        for (int i = 0; i < 3; i++) {
            var config = NativeCompatibility.enrolledBuildRoles(config(zones.get(i)));
            nodes.add(ActorSystem.create(Behaviors.empty(), "cell-shape-test", config));
        }
        var seed = Cluster.get(nodes.getFirst()).selfMember().address();
        for (var node : nodes) Cluster.get(node).manager().tell(new JoinSeedNodes(List.of(seed)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!allUp()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Cluster did not reach three Up members");
            Thread.sleep(100);
        }
    }

    @AfterAll
    static void stop() throws Exception {
        for (var node : nodes) node.terminate();
        for (var node : nodes) node.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
        nodes.clear();
    }

    static boolean allUp() {
        return nodes.stream()
                .allMatch(
                        node ->
                                java.util.stream.StreamSupport.stream(
                                                Cluster.get(node).state().getMembers().spliterator(), false)
                                        .filter(member -> member.status().equals(MemberStatus.up()))
                                        .count() == 3);
    }

    static com.typesafe.config.Config config(String zone) {
        return ConfigFactory.parseString(
                        """
                                pekko.actor.provider=cluster
                                pekko.remote.artery.transport=tcp
                                pekko.remote.artery.canonical.hostname="127.0.0.1"
                                pekko.remote.artery.canonical.port=0
                                pekko.cluster.roles=["signaling-actor","az-%s"]
                                pekko.cluster.jmx.multi-mbeans-in-same-jvm=on
                                pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
                                pekko.loglevel=WARNING
                                signaling.cell-id="c001"
                                signaling.cluster-fingerprint="%s"
                                signaling.compatibility {
                                  major=1
                                  minor=2
                                  executable-id="%s"
                                  features=["session-s1"]
                                  window { major=1, lower-minor=1, upper-minor=2 }
                                }
                                """
                                .formatted(zone, FINGERPRINT, EXECUTABLE))
                .resolve();
    }

    static NativeCompatibility compatibility(ClusterShape shape) {
        var system = nodes.getFirst();
        var business = new NativeActorBusinessEnrollment(system, "c001", 1, 1, null, null, null, null, null);
        var scheduling =
                new NativeActorSchedulingEnrollment(
                        system,
                        Set.of("az-a", "az-b"),
                        FINGERPRINT,
                        List.of(),
                        event -> {
                        },
                        new InetSocketAddress("127.0.0.1", 0),
                        () -> true,
                        () -> "");
        return new NativeCompatibility(
                "c001", FINGERPRINT, () -> business, () -> scheduling, () -> null, shape);
    }

    @Test
    void aDeclaredThreeMemberCellIsCompatibleAtItsQuorum() {
        assertThat(compatibility(ClusterShape.localMajority(3)).permits("c001", "session-s1")).isTrue();
    }

    @Test
    void theProductionShapeStillRefusesThreeUpMembers() {
        assertThat(compatibility(ClusterShape.PRODUCTION).permits("c001", "session-s1")).isFalse();
    }
}
