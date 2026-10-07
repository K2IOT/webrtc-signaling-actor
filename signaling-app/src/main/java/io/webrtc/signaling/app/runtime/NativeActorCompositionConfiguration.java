package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.ClusterReadiness;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.NativeActorComposition;
import io.webrtc.signaling.storage.NativeActorSecurityPolicies;
import io.webrtc.signaling.storage.SqlTransactions;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Installs one native backend composition; region registration still requires real membership Up. */
@AutoConfiguration(after=NativeActorPolicyConfiguration.class)
@Profile("actor")
@ConditionalOnBean({NativeActorBusinessEnrollment.class, SqlTransactions.class,
    NativeActorSecurityPolicies.class, ClockSafetyMonitor.class})
public class NativeActorCompositionConfiguration {
    @Bean @ConditionalOnMissingBean ClusterReadiness nativeClusterReadiness() { return new ClusterReadiness(); }

    @Bean NativeActorComposition nativeActorComposition(NativeActorBusinessEnrollment enrollment, SqlTransactions sql,
            NativeActorSecurityPolicies security, ClockSafetyMonitor clock, ClusterReadiness readiness,
            ObjectProvider<NativeActorSourceEnrollment> sources) {
        var source=sources.getIfAvailable();
        if(source!=null&&(!enrollment.cell().equals(source.cell())||enrollment.storageEpoch()!=source.storageEpoch()
                ||!enrollment.podUid().equals(source.podUid())))
            throw new IllegalArgumentException("Native actor authority differs from source enrollment");
        if(!enrollment.cell().equals(enrollment.system().settings().config().getString("signaling.cell-id")))
            throw new IllegalArgumentException("Native actor cell differs from enrolled process");
        var inputs=new NativeActorComposition.Inputs(sql,enrollment.cell(),enrollment.storageEpoch(),enrollment.routingEpoch(),
            enrollment.podUid(),enrollment.proofs(),enrollment.homes(),security,security,enrollment.epochAdoption(),
            enrollment.tokens(),enrollment.callingPolicy(),Clock.systemUTC(),clock::valid,security);
        return new NativeActorComposition(enrollment.system(),inputs,readiness);
    }

    @Bean ShardingBootstrap.Regions nativeActorRegions(NativeActorComposition actors) {
        // The native bootstrap enforces actual local Up and the installed lease provider.
        // An enrolled but not-yet-Up process cannot bind its business RPC listener.
        return actors.register();
    }
}
