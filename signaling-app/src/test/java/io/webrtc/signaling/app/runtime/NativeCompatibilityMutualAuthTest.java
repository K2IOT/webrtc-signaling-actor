package io.webrtc.signaling.app.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One real four-member, two-zone cluster; each member declares the mutual-authentication flag at a
 * different key path, and each is used in turn as the local system whose gate is read. Transport is
 * plain TCP: the flag is only a declared policy that the gate reads, which is what is under test.
 */
class NativeCompatibilityMutualAuthTest {
    static final String FINGERPRINT = "a".repeat(64);
    static final String EXECUTABLE = "b".repeat(64);
    static final String ENGINE = "pekko.remote.artery.ssl.config-ssl-engine.require-mutual-authentication";
    static final String LEGACY = "pekko.remote.artery.ssl.config.require-mutual-authentication";
    static final List<ActorSystem<Void>> nodes = new ArrayList<>();
    // Member 0: engine path only (Pekko's own default is also on). 1: legacy path only, engine left
    // at its default. 2: engine off. 3: engine off while the legacy path says on.
    static final List<String> flags =
            List.of(ENGINE + "=on", LEGACY + "=on", ENGINE + "=off", ENGINE + "=off\n" + LEGACY + "=on");
    static final List<String> zones = List.of("a", "a", "b", "b");

    @BeforeAll
    static void formCluster() throws Exception {
        for (int i = 0; i < 4; i++) {
            var config = NativeCompatibility.enrolledBuildRoles(config(zones.get(i), flags.get(i)));
            nodes.add(ActorSystem.create(Behaviors.empty(), "mutual-auth-test", config));
        }
        var seed = Cluster.get(nodes.getFirst()).selfMember().address();
        for (var node : nodes) Cluster.get(node).manager().tell(new JoinSeedNodes(List.of(seed)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!allUp()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Cluster did not reach four Up members");
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
                                Cluster.get(node).state().getMembers().spliterator().getExactSizeIfKnown() != 0
                                        && java.util.stream.StreamSupport.stream(
                                                Cluster.get(node).state().getMembers().spliterator(), false)
                                        .filter(member -> member.status().equals(MemberStatus.up()))
                                        .count() == 4);
    }

    static Config config(String zone, String mutualAuth) {
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
                                %s
                                """
                                .formatted(zone, FINGERPRINT, EXECUTABLE, mutualAuth))
                .resolve();
    }

    static boolean permits(int local) {
        var system = nodes.get(local);
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
        return new NativeCompatibility("c001", FINGERPRINT, () -> business, () -> scheduling, () -> null)
                .permits("c001", "session-s1");
    }

    @Test
    void permitsWhenMutualAuthIsSetUnderConfigSslEngine() {
        assertThat(permits(0)).isTrue();
    }

    /**
     * Not a proof of the fallback: Pekko's reference.conf defines the effective key (on), so this
     * member passes on the default. The bare-Config test below proves the fallback itself.
     */
    @Test
    void permitsWhenOnlyTheLegacyKeyIsSetAndTheEffectiveKeyIsLeftAtPekkoDefault() {
        assertThat(permits(1)).isTrue();
    }

    @Test
    void refusesWhenTheEffectivePathTurnsMutualAuthOff() {
        assertThat(permits(2)).isFalse();
    }

    @Test
    void fallsBackToTheLegacyPathOnlyWhenTheEffectiveOneIsAbsentAndFailsClosedOtherwise() {
        var bare = ConfigFactory.parseString("a=1");
        assertThatThrownBy(() -> NativeCompatibility.mutualAuthenticationRequired(bare))
                .isInstanceOf(com.typesafe.config.ConfigException.class);
        assertThat(
                NativeCompatibility.mutualAuthenticationRequired(
                        ConfigFactory.parseString(LEGACY + "=on")))
                .isTrue();
        assertThat(
                NativeCompatibility.mutualAuthenticationRequired(
                        ConfigFactory.parseString(ENGINE + "=off\n" + LEGACY + "=on")))
                .isFalse();
    }

    @Test
    void theEffectivePathWinsOverTheLegacyOne() {
        assertThat(permits(3)).isFalse();
    }
}
