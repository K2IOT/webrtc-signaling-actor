package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.CellRpcClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;

/** Build one lazy native client; the plane lifecycle owns its original physical drain. */
@AutoConfiguration(before={NativeActorIngressConfiguration.class,NativeGatewayBusinessConfiguration.class})
@Profile({"actor","gateway"})
@ConditionalOnBean(NativeCellRpcEnrollment.class)
public class NativeCellRpcConfiguration {
    @Bean(destroyMethod="") @ConditionalOnMissingBean(CellRpcClient.class)
    CellRpcClient nativeCellRpcClient(NativeCellRpcEnrollment enrollment){
        return new CellRpcClient(enrollment.environment(),enrollment.destinations(),enrollment.tls(),enrollment.admission());
    }
}
