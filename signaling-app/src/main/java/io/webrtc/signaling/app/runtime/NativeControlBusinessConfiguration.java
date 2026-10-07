package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.control.*;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;

@AutoConfiguration
@Profile("control")
@ConditionalOnBean(NativeControlBusinessEnrollment.class)
public class NativeControlBusinessConfiguration {
    @Bean DirectoryService nativeControlDirectory(NativeControlBusinessEnrollment inputs){return new DirectoryService(inputs.directory(),16384,inputs.directoryRefresh());}
    @Bean(destroyMethod="close") @ConditionalOnMissingBean(BoundedTokenVerifier.class)
    BoundedTokenVerifier nativeControlTokens(NativeControlBusinessEnrollment inputs){return inputs.tokens();}
    @Bean BootstrapController nativeControlBootstrap(NativeControlBusinessEnrollment inputs,DirectoryService directory,NativeControlReadiness readiness,BoundedTokenVerifier tokens){
        if(tokens!=inputs.tokens())throw new IllegalArgumentException("Native control verifier ownership differs");
        return new BootstrapController(directory,tokens,inputs.cachedSecurity(),Clock.systemUTC(),readiness::safe);
    }
    @Bean NativeControlReadiness nativeControlReadiness(NativeControlBusinessEnrollment inputs){return new NativeControlReadiness(inputs);}
}
