package io.webrtc.signaling.app;

import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/** Startup failure retires only already-created native gateway owners, in physical order. */
final class NativeStartupCleanup implements BeanPostProcessor {
    private final List<Object> created=new ArrayList<>();
    @Override public synchronized Object postProcessAfterInitialization(Object bean,String name){
        if(bean instanceof NativeGatewayIngress||bean instanceof NativeGatewaySpringLifecycle
                ||bean instanceof NativeRelaySessionProofCache||bean instanceof CellRpcClient||bean instanceof GatewayBootController||bean instanceof BoundedTokenVerifier)
            if(created.stream().noneMatch(owner->owner==bean))created.add(bean);
        return bean;
    }
    synchronized void failed(ApplicationContext context,Throwable failed){gateway(context,failed,List.copyOf(created));}
    static void gateway(ApplicationContext context,Throwable failed){
        if(!(context instanceof ConfigurableApplicationContext configurable))return;
        var beans=configurable.getBeanFactory();
        var owners=java.util.Arrays.stream(beans.getSingletonNames()).map(beans::getSingleton).toList();
        gateway(context,failed,owners);
    }
    private static void gateway(ApplicationContext context,Throwable failed,List<Object> owners){
        if(context==null||!List.of(context.getEnvironment().getActiveProfiles()).contains("gateway"))return;
        var lifecycle=owners.stream().filter(NativeGatewaySpringLifecycle.class::isInstance).map(NativeGatewaySpringLifecycle.class::cast).findFirst();
        long started=System.nanoTime();
        try{
            if(lifecycle.isPresent())lifecycle.get().drain().toCompletableFuture().get(310,TimeUnit.SECONDS);
            else{
                for(var owner:owners)if(owner instanceof NativeGatewayIngress ingress)await(ingress.drain(),started);
                for(var owner:owners)if(owner instanceof NativeRelaySessionProofCache cache)await(cache.drain(),started);
                for(var owner:owners)if(owner instanceof BoundedTokenVerifier tokens)await(tokens.drain(),started);
                for(var owner:owners)if(owner instanceof CellRpcClient client)await(client.drain(),started);
                for(var owner:owners)if(owner instanceof GatewayBootController boot)boot.close();
            }
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();failed.addSuppressed(new IllegalStateException("Native startup cleanup interrupted"));}
        catch(Exception unknown){failed.addSuppressed(new IllegalStateException("Native startup cleanup unproven"));}
    }
    private static void await(CompletionStage<Void> completion,long started)throws Exception {
        long remaining=TimeUnit.SECONDS.toNanos(300)-(System.nanoTime()-started);
        if(remaining<=0)throw new TimeoutException();
        completion.toCompletableFuture().get(remaining,TimeUnit.NANOSECONDS);
    }
}
