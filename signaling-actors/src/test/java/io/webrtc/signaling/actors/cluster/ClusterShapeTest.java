package io.webrtc.signaling.actors.cluster;

import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The cell shape is production's six members with a quorum of four unless a LOCAL_TEST_ONLY
 * process declares a smaller cell, whose quorum is then a strict majority of its members.
 */
class ClusterShapeTest {
    private static final String CONTACT_POINTS =
            "pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr";
    private static final String QUORUM = "pekko.cluster.role.signaling-actor.min-nr-of-members";

    private static com.typesafe.config.Config productionConfig() {
        return ShardingConfigurationTest.config();
    }

    @Test
    @DisplayName("production is six members with a quorum of four")
    void production_isSixWithQuorumFour() {
        assertThat(ClusterShape.PRODUCTION.members()).isEqualTo(6);
        assertThat(ClusterShape.PRODUCTION.quorum()).isEqualTo(4);
        assertThat(ClusterShape.localMajority(6)).isEqualTo(ClusterShape.PRODUCTION);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    @DisplayName("a local cell's quorum is a strict majority of its members")
    void localMajority_isStrictMajority(int members) {
        var shape = ClusterShape.localMajority(members);

        assertThat(shape.members()).isEqualTo(members);
        assertThat(shape.quorum()).isEqualTo(members / 2 + 1);
        assertThat(shape.quorum() * 2).isGreaterThan(members);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 7, 100})
    @DisplayName("a local cell outside two to six members is refused")
    void localMajority_outOfRange_isRefused(int members) {
        assertThatThrownBy(() -> ClusterShape.localMajority(members))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("validateProduction without a shape still pins six members and quorum four")
    void validateProduction_default_keepsProductionPins() {
        ShardingBootstrap.validateProduction(productionConfig());

        assertThatThrownBy(
                () ->
                        ShardingBootstrap.validateProduction(
                                ConfigFactory.parseString(QUORUM + "=2," + CONTACT_POINTS + "=3")
                                        .withFallback(productionConfig())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a declared three-member shape accepts exactly its own quorum and contact points")
    void validateProduction_threeMemberShape_acceptsOnlyItsOwnPins() {
        var shape = ClusterShape.localMajority(3);
        var three =
                ConfigFactory.parseString(
                                "pekko.cluster.min-nr-of-members=2,"
                                        + QUORUM + "=2,"
                                        + CONTACT_POINTS + "=3")
                        .withFallback(productionConfig());

        ShardingBootstrap.validateProduction(three, shape);

        // production pins under a smaller declared shape, and the reverse, are both refused
        assertThatThrownBy(() -> ShardingBootstrap.validateProduction(productionConfig(), shape))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShardingBootstrap.validateProduction(three, ClusterShape.PRODUCTION))
                .isInstanceOf(IllegalArgumentException.class);
        // a quorum or contact count that disagrees with the declared shape is refused
        assertThatThrownBy(
                () ->
                        ShardingBootstrap.validateProduction(
                                ConfigFactory.parseString(QUORUM + "=1").withFallback(three), shape))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () ->
                        ShardingBootstrap.validateProduction(
                                ConfigFactory.parseString(CONTACT_POINTS + "=2").withFallback(three), shape))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a smaller shape does not relax any other production pin")
    void validateProduction_smallerShape_keepsOtherPins() {
        var shape = ClusterShape.localMajority(3);
        var three =
                ConfigFactory.parseString(
                                "pekko.cluster.min-nr-of-members=2,"
                                        + QUORUM + "=2,"
                                        + CONTACT_POINTS + "=3")
                        .withFallback(productionConfig());

        for (var weaker :
                new String[]{
                        "pekko.remote.artery.ssl.config-ssl-engine.require-mutual-authentication=off",
                        "pekko.remote.artery.ssl.config-ssl-engine.protocol=TLSv1.2",
                        "pekko.cluster.allow-weakly-up-members=on",
                        "pekko.cluster.configuration-compatibility-check.enforce-on-join=off"})
            assertThatThrownBy(
                    () ->
                            ShardingBootstrap.validateProduction(
                                    ConfigFactory.parseString(weaker).withFallback(three), shape))
                    .as(weaker)
                    .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the compatibility window admits quorum up to members plus one surge member")
    void admitsPeerCount_isQuorumThroughMembersPlusOne() {
        var production = ClusterShape.PRODUCTION;
        for (int peers = 0; peers <= 9; peers++)
            assertThat(production.admitsPeerCount(peers)).as("production %s", peers).isEqualTo(peers >= 4 && peers <= 7);
        var three = ClusterShape.localMajority(3);
        for (int peers = 0; peers <= 9; peers++)
            assertThat(three.admitsPeerCount(peers)).as("three %s", peers).isEqualTo(peers >= 2 && peers <= 4);
        var two = ClusterShape.localMajority(2);
        for (int peers = 0; peers <= 9; peers++)
            assertThat(two.admitsPeerCount(peers)).as("two %s", peers).isEqualTo(peers >= 2 && peers <= 3);
    }

    @Test
    @DisplayName("management binding validates against the declared shape, not production's")
    void bindManagement_validatesAgainstTheDeclaredShape() throws Exception {
        var shape = ClusterShape.localMajority(3);
        var three =
                ConfigFactory.parseString(
                                "pekko.actor.provider=local,"
                                        + "pekko.cluster.min-nr-of-members=2,"
                                        + QUORUM + "=2,"
                                        + CONTACT_POINTS + "=3")
                        .withFallback(ShardingConfigurationTest.config());
        var system = ActorSystem.create(Behaviors.<Void>empty(), "shape-test", three);
        try {
            // a matching shape passes validation and only then trips on the missing TLS contexts
            assertThatThrownBy(() -> ShardingBootstrap.bindManagement(system, null, null, shape))
                    .isInstanceOf(NullPointerException.class);
            // the production overload still demands six members and a quorum of four
            assertThatThrownBy(() -> ShardingBootstrap.bindManagement(system, null, null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                    () -> ShardingBootstrap.bindManagement(system, null, null, ClusterShape.PRODUCTION))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
