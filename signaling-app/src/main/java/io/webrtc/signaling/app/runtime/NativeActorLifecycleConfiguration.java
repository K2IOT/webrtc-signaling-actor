package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.PekkoShutdownLifecycle;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.util.ArrayList;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.support.DefaultLifecycleProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Main installs native ownership in Pekko's graph before starting fixed workers. */
@AutoConfiguration(after=NativeActorIngressConfiguration.class)
@Profile("actor")
@ConditionalOnBean({NativeActorComposition.class,CellRpcServer.class,CellRpcClient.class,
    NativeWorkerScheduler.class,DbBoundary.class,DbPools.class,PrivateHealthServer.class})
public class NativeActorLifecycleConfiguration {
    @Bean(name="lifecycleProcessor") @ConditionalOnMissingBean(name="lifecycleProcessor")
    static DefaultLifecycleProcessor nativeLifecycleProcessor(){
        var lifecycle=new DefaultLifecycleProcessor();lifecycle.setTimeoutPerShutdownPhase(70_000);return lifecycle;
    }
    @Bean NativeActorSpringLifecycle nativeActorSpringLifecycle(NativeActorComposition actors,NativeActorRuntimeHooks hooks){
        return new NativeActorSpringLifecycle(actors.system(),hooks);
    }
    @Bean(destroyMethod="") NativeActorRuntimeHooks nativeActorRuntimeHooks(NativeActorComposition actors,
            CellRpcServer server,CellRpcClient client,NativeWorkerScheduler workers,DbBoundary database,
            DbPools pools,PrivateHealthServer health,ObjectProvider<NativeActorSafetySources> sources,
            ObjectProvider<NativeActorRpcIngress> ingress) {
        if(PekkoShutdownLifecycle.totalBudget(actors.system().settings().config()).compareTo(Duration.ofSeconds(65))>0)
            throw new IllegalArgumentException("Shutdown graph exceeds 65s");
        var drains=new ArrayList<Supplier<CompletionStage<Void>>>();
        var listener=ingress.getIfAvailable();
        if(listener!=null){
            if(listener.server()!=server)throw new IllegalArgumentException("Native actor listener ownership differs");
            drains.add(listener::drain);
        }
        var safety=sources.getIfAvailable();if(safety!=null)drains.add(safety::drain);
        var hooks=new NativeActorRuntimeHooks(actors,server,client,workers,database,pools,health,drains);
        PekkoShutdownLifecycle.register(actors.system(),hooks);
        workers.start();
        return hooks;
    }
}
