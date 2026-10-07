package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.GatewayRelayRpcServer;
import java.time.Clock;
import java.util.concurrent.*;

/** One owner for native WSS, relay writes and their bounded CPU executor. */
public final class NativeGatewayIngress implements AutoCloseable {
    private final ConnectionRegistry connections;
    private final GatewayServer wss;
    private final GatewayRelayStream writes;
    private final GatewayRelayRpcServer relay;
    private final ExecutorService cpu=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(256),
        Thread.ofPlatform().daemon().name("gateway-relay-json-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    private boolean draining;
    NativeGatewayIngress(NativeGatewayBusinessEnrollment business,NativeGatewayIngressEnrollment inputs,
            NativeGatewayServices services,ClockSafetyMonitor clock)throws Exception {
        var identity=business.identity();
        connections=new ConnectionRegistry(identity.gatewayId(),identity.bootId(),inputs.maxUnauthenticated(),inputs.maxConnections());
        wss=new GatewayServer(inputs.wssTls(),inputs.upgradePolicy(),connections,services,Clock.systemUTC(),inputs.eventLoops(),inputs.edgeLimits(),inputs.securitySweep());
        writes=new GatewayRelayStream(connections,services,Clock.systemUTC(),cpu,inputs.writeAdmission(),clock::valid);
        relay=new GatewayRelayRpcServer(identity.cell(),identity.gatewayId(),identity.bootId(),inputs.rpcEnvironment(),
            inputs.rpcPort(),inputs.rpcTls(),inputs.rpcAdmission(),writes::send);
        try{relay.start();wss.start(inputs.wssAddress());}
        catch(Exception failed){drain();throw failed;}
    }
    public GatewayServer wss(){return wss;}
    public GatewayRelayRpcServer relay(){return relay;}
    public ConnectionRegistry connections(){return connections;}
    public synchronized CompletionStage<Void> drain(){
        if(!draining){
            draining=true;
            wss.stopAccepting();
            relay.drain().thenCombine(writes.drain(),(a,b)->null).whenComplete((ignored,failure)->{
                if(failure!=null){drained.completeExceptionally(failure);return;}
                Thread.startVirtualThread(()->{
                    try{wss.close();cpu.shutdown();if(!cpu.awaitTermination(2,TimeUnit.SECONDS))throw new TimeoutException("Native gateway CPU cleanup unproven");drained.complete(null);}
                    catch(Exception unknown){if(unknown instanceof InterruptedException)Thread.currentThread().interrupt();drained.completeExceptionally(unknown);}
                });
            });
        }
        return drained.minimalCompletionStage();
    }
    @Override public void close(){drain();}
}
