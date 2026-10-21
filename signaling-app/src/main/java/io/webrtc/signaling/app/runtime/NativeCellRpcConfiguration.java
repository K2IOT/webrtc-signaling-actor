package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.CellRpcClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Build one lazy native client; the plane lifecycle owns its original physical drain. On the control
 * plane the client presents the control workload identity, which actor servers admit for
 * compatibility observation only, and {@link NativeControlProcess} owns its drain.
 */
@AutoConfiguration(
        before = {
                NativeActorIngressConfiguration.class,
                NativeGatewayBusinessConfiguration.class,
                NativeControlBusinessConfiguration.class
        })
@Profile({"actor", "gateway", "control"})
@ConditionalOnBean(NativeCellRpcEnrollment.class)
public class NativeCellRpcConfiguration {
    @Bean(destroyMethod = "")
    @ConditionalOnMissingBean(CellRpcClient.class)
    CellRpcClient nativeCellRpcClient(
            NativeCellRpcEnrollment enrollment,
            NativeTelemetry telemetry,
            NativeCompatibility compatibility) {
        return new CellRpcClient(
                enrollment.environment(),
                enrollment.destinations(),
                enrollment.tls(),
                enrollment.admission())
                .observe(telemetry)
                .compatibility(compatibility);
    }
}
