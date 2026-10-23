package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ClusterShape;
import io.webrtc.signaling.app.CompatibilityWindow;
import io.webrtc.signaling.protocol.internal.CompatibilityActorDescriptor;
import io.webrtc.signaling.protocol.internal.CompatibilityObservationReply;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The actor count a cell may report to a gateway or control follows the declared cell shape, and
 * stays four to seven for production.
 */
class NativeCompatibilityCollectorsShapeTest {
    private static final String FINGERPRINT = "a".repeat(64);
    private static final String EXECUTABLE = "b".repeat(64);

    private static NativeCompatibilityCollectors collectors(ClusterShape shape) {
        var inputs = mock(NativeCompatibilityObservationEnrollment.class);
        when(inputs.installedBuild())
                .thenReturn(
                        new NativeCompatibilityEnrollment.BuildIdentity(
                                1, 2, EXECUTABLE, FINGERPRINT, Set.of(CompatibilityWindow.Feature.SESSION_S1)));
        return new NativeCompatibilityCollectors(inputs, () -> null, () -> null, () -> null, shape);
    }

    private static CompatibilityObservationReply reply(int actors, int zones) {
        var reply =
                CompatibilityObservationReply.newBuilder()
                        .setSourceCell("c002")
                        .setValidUntilMs(Instant.now().plusSeconds(5).toEpochMilli());
        for (int i = 0; i < actors; i++)
            reply.addActors(
                    CompatibilityActorDescriptor.newBuilder()
                            .setMemberId(UUID.randomUUID().toString())
                            .setZone("az-" + (char) ('a' + i % zones))
                            .setCell("c002")
                            .setMajor(1)
                            .setMinor(2)
                            .setExecutableId(EXECUTABLE)
                            .setFingerprint(FINGERPRINT)
                            .addFeatures("SESSION_S1"));
        return reply.build();
    }

    private static boolean accepts(ClusterShape shape, int actors) {
        try {
            collectors(shape).acceptActors("c002", reply(actors, 2));
            return true;
        } catch (IllegalStateException refused) {
            return false;
        }
    }

    @Test
    @DisplayName("production accepts four to seven actors and refuses everything else")
    void production_acceptsFourToSeven() {
        for (int actors = 0; actors <= 9; actors++)
            assertThat(accepts(ClusterShape.PRODUCTION, actors))
                    .as("production %s", actors)
                    .isEqualTo(actors >= 4 && actors <= 7);
    }

    @Test
    @DisplayName("a declared three-actor cell accepts its quorum up to one surge member")
    void threeActorCell_acceptsTwoToFour() {
        var three = ClusterShape.localMajority(3);
        for (int actors = 0; actors <= 9; actors++)
            assertThat(accepts(three, actors)).as("three %s", actors).isEqualTo(actors >= 2 && actors <= 4);
    }

    @Test
    @DisplayName("a smaller cell keeps the two-zone requirement")
    void smallerCell_stillNeedsTwoZones() {
        var collectors = collectors(ClusterShape.localMajority(3));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> collectors.acceptActors("c002", reply(3, 1)))
                .isInstanceOf(IllegalStateException.class);
    }
}
