package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.rpc.NativeActorComposition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@AutoConfiguration(after = NativeActorCompositionConfiguration.class)
@Profile("actor")
@ConditionalOnBean(NativeRecoveryEnrollment.class)
public class NativeRecoveryAccountingConfiguration {
    @Bean(destroyMethod = "")
    NativeRecoveryAccounting nativeRecoveryAccounting(
            NativeActorComposition actors,
            NativeRecoveryEnrollment enrollment,
            SignalingMetrics metrics,
            NativeTelemetry telemetry) {
        return new NativeRecoveryAccounting(actors, enrollment, metrics, telemetry);
    }
}
