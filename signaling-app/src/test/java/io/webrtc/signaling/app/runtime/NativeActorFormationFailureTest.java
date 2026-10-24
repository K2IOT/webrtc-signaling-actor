package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A member whose cluster fingerprint (or another join-checked key) differs is refused by the
 * cluster, and Pekko shuts its system down with IncompatibleConfigurationDetected. Startup must say
 * so with the fixed identity vocabulary instead of waiting 30 s for a generic formation timeout.
 */
class NativeActorFormationFailureTest {
    @Test
    void anIncompatibleJoinIsAnIdentityMismatch() {
        var failure =
                NativeActorProcess.formationFailure(
                        Optional.of(CoordinatedShutdown.incompatibleConfigurationDetectedReason()));
        assertThat(failure)
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        local -> {
                            assertThat(local.role()).isEqualTo(LocalRuntimeFailure.Role.IDENTITY);
                            assertThat(local.category())
                                    .isEqualTo(LocalRuntimeFailure.Category.IDENTITY_MISMATCH);
                        });
    }

    @Test
    void otherShutdownsKeepTheGenericFormationFailure() {
        assertThat(
                NativeActorProcess.formationFailure(
                        Optional.of(CoordinatedShutdown.clusterJoinUnsuccessfulReason())))
                .isNull();
        assertThat(NativeActorProcess.formationFailure(Optional.empty())).isNull();
    }
}
