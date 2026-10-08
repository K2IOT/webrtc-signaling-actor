package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.CallAuthorizationPolicy;
import java.time.Duration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** The selected business rule is owned and versioned here; identity trust still requires enrollment. */
@AutoConfiguration(after=NativeActorSourceConfiguration.class,before=NativeActorCompositionConfiguration.class)
@Profile("actor")
@ConditionalOnBean(NativeActorSourceEnrollment.class)
public class NativeCallingPolicyConfiguration {
    public static final String VERSION="open-authenticated-v1";
    @Bean @ConditionalOnMissingBean(CallAuthorizationPolicy.class)
    CallAuthorizationPolicy nativeCallingPolicy(NativeActorSourceEnrollment enrollment) {
        var age=enrollment.identity().hardSafetyBound();
        if(age.compareTo(Duration.ofSeconds(5))>0)age=Duration.ofSeconds(5);
        return CallAuthorizationPolicy.openAuthenticated(VERSION,age);
    }
}
