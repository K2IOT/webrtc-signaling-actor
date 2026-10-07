package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.CellRpcClient;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Native gateway composition with no implicit identity, directory or trusted-clock defaults. */
@AutoConfiguration
@Profile("gateway")
@ConditionalOnBean({NativeGatewayBusinessEnrollment.class, CellRpcClient.class, ClockSafetyMonitor.class})
public class NativeGatewayBusinessConfiguration {
    // A gateway lifecycle retires these owners after original transport receipts settle.
    @Bean(destroyMethod="") GatewayBootController nativeGatewayBoot(NativeGatewayBusinessEnrollment enrollment, CellRpcClient client) {
        var boot=new GatewayBootController(enrollment.identity(),GatewayBootController.network(client),System::nanoTime);
        boot.start();
        return boot;
    }
    @Bean(destroyMethod="") NativeRelaySessionProofCache nativeGatewayRelayProofCache(NativeGatewayBusinessEnrollment enrollment,
            GatewayBootController boot, ClockSafetyMonitor clock) {
        return new NativeRelaySessionProofCache(enrollment.relayCacheCapacity(),enrollment.relayPendingLimit(),
            Clock.systemUTC(),System::nanoTime,()->boot.current()&&clock.valid(),enrollment.relayProofs());
    }
    @Bean NativeGatewayCommands nativeGatewayCommands(NativeGatewayBusinessEnrollment enrollment,
            CellRpcClient client, NativeRelaySessionProofCache cache) {
        return new NativeGatewayCommands(enrollment.identity(),enrollment.homes(),NativeGatewayCommands.network(client),
            Clock.systemUTC(),enrollment.routingEpoch()).relayProofCache(cache);
    }
    @Bean NativeGatewayServices nativeGatewayServices(NativeGatewayBusinessEnrollment enrollment,
            GatewayBootController boot, ClockSafetyMonitor clock, CellRpcClient client, NativeGatewayCommands commands) {
        return new NativeGatewayServices(boot,enrollment.tokens(),
            (principal,now)->clock.valid()?enrollment.cachedSecurity().apply(principal,now):AuthorizationStatus.FRESHNESS_UNKNOWN,
            user->{var home=enrollment.homes().apply(user);return new NativeGatewayServices.Home(home.cell(),home.directoryEpoch());},
            client,commands);
    }
}
