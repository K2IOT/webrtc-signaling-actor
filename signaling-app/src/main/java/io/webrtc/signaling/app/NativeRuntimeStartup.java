package io.webrtc.signaling.app;

import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.control.*;
import io.webrtc.signaling.gateway.GatewayServer;
import io.webrtc.signaling.rpc.*;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.web.context.WebServerApplicationContext;

/** Refuse a successful process launch from a configuration-only Spring context. */
public final class NativeRuntimeStartup {
    private NativeRuntimeStartup(){}
    static final class MissingRuntime extends IllegalStateException {
        MissingRuntime(SignalingApplication.Plane plane){super("Native runtime not installed: "+plane.name());}
    }
    public static IllegalStateException missing(SignalingApplication.Plane plane){return new MissingRuntime(plane);}
    static void requireInstalled(SignalingApplication.Plane plane,ApplicationContext context){
        try {
            switch(plane){
                case ACTOR -> {
                    one(context,NativeActorSpringLifecycle.class,plane);
                    one(context,NativeActorRuntimeHooks.class,plane);
                    var actors=one(context,NativeActorComposition.class,plane);
                    if(!actors.readiness().snapshot().regionsRegistered())throw new MissingRuntime(plane);
                    one(context,NativeActorSafetySources.class,plane);
                    one(context,NativeWorkerScheduler.class,plane);
                    bound(one(context,NativeActorRpcIngress.class,plane).server().port(),plane);
                }
                case GATEWAY -> {
                    one(context,NativeGatewaySpringLifecycle.class,plane);
                    one(context,CellRpcClient.class,plane);
                    bound(one(context,GatewayServer.class,plane).port(),plane);
                    bound(one(context,GatewayRelayRpcServer.class,plane).port(),plane);
                }
                case CONTROL -> {
                    one(context,DirectoryService.class,plane);
                    one(context,BoundedTokenVerifier.class,plane);
                    one(context,NativeControlBusinessEnrollment.class,plane);
                    one(context,BootstrapController.class,plane);
                    if(!(context instanceof WebServerApplicationContext web)||web.getWebServer()==null)throw new MissingRuntime(plane);
                    bound(web.getWebServer().getPort(),plane);
                }
            }
            bound(one(context,PrivateHealthServer.class,plane).port(),plane);
        }catch(MissingRuntime missing){throw missing;}
        catch(RuntimeException unstarted){throw new MissingRuntime(plane);}
    }
    private static <T> T one(ApplicationContext context,Class<T> type,SignalingApplication.Plane plane){
        var values=context.getBeansOfType(type);
        if(values.size()!=1)throw new MissingRuntime(plane);
        return values.values().iterator().next();
    }
    private static void bound(int port,SignalingApplication.Plane plane){if(port<1||port>65535)throw new MissingRuntime(plane);}
}
