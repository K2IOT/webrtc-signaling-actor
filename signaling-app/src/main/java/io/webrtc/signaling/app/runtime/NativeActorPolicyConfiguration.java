package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.storage.NativeActorSecurityPolicies;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.*;

/** Install actor policies only from the explicit native source and live clock owners. */
@AutoConfiguration
@Profile("actor")
@ConditionalOnBean({RevocationReconciler.class,ClockSafetyMonitor.class})
public class NativeActorPolicyConfiguration {
    @Bean NativeActorSecurityPolicies nativeActorSecurityPolicies(RevocationReconciler source,ClockSafetyMonitor clock){
        return new NativeActorSecurityPolicies(source,clock::valid);
    }
}
