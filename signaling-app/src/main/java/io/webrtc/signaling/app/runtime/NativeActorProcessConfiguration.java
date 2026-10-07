package io.webrtc.signaling.app.runtime;

import org.apache.pekko.actor.typed.ActorSystem;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Explicit PKI/config enrollment creates one process; business dependencies receive only local Up. */
@AutoConfiguration(before={NativeActorSourceConfiguration.class,NativeActorCompositionConfiguration.class})
@Profile("actor")
@ConditionalOnBean(NativeActorProcessEnrollment.class)
public class NativeActorProcessConfiguration {
    @Bean(destroyMethod="") NativeActorProcess nativeActorProcess(NativeActorProcessEnrollment enrollment){
        return new NativeActorProcess(enrollment);
    }
    @Bean(destroyMethod="") ActorSystem<?> nativeActorSystem(NativeActorProcess process){return process.awaitLocalUp();}
}
