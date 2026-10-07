package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.NativeActorComposition;
import io.webrtc.signaling.rpc.NativeActorRpcIngress;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import java.io.IOException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.ObjectProvider;

/** Bind only after native regions are registered and all internal transport/workload inputs exist. */
@AutoConfiguration(after=NativeActorCompositionConfiguration.class)
@Profile("actor")
@ConditionalOnBean({NativeActorComposition.class, NativeActorIngressEnrollment.class})
public class NativeActorIngressConfiguration {
    // NativeActorRuntimeHooks must retire this owner after ingress work and native roots settle.
    @Bean(destroyMethod="") NativeActorRpcIngress nativeActorRpcIngress(NativeActorComposition actors,
            NativeActorIngressEnrollment enrollment, ObjectProvider<ShardingBootstrap.Regions> regions) throws IOException {
        // Resolve Main's registration dependency before listener creation. Already-installed
        // native compositions still undergo rpcIngress's independent registered-region guard.
        regions.getIfAvailable();
        return actors.rpcIngress(enrollment.environment(),enrollment.port(),enrollment.tls(),enrollment.admission(),
            enrollment.network(),enrollment.relayCapacity(),enrollment.relayMemory(),enrollment.gateways(),
            enrollment.gatewayWorkloads(),enrollment.delivery()).start();
    }
}
