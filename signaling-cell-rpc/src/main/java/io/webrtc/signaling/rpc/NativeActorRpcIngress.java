package io.webrtc.signaling.rpc;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Fully installed native actor listener and its owned volatile relay transport. */
public final class NativeActorRpcIngress implements AutoCloseable {
    private final CellRpcServer server;
    private final NativeRelayProducer relay;
    private final GatewayRelayRpcClient gateway;
    private boolean started,draining;
    private CompletionStage<Void> drained;
    NativeActorRpcIngress(CellRpcServer server,NativeRelayProducer relay,GatewayRelayRpcClient gateway) {
        this.server=Objects.requireNonNull(server);this.relay=Objects.requireNonNull(relay);this.gateway=Objects.requireNonNull(gateway);
    }
    public synchronized NativeActorRpcIngress start() throws IOException {
        if(started||draining)throw new IllegalStateException("Native actor ingress lifecycle");
        started=true;
        try{server.start();return this;}catch(IOException|RuntimeException failed){drain();throw failed;}
    }
    public CellRpcServer server(){return server;}
    /** Register this owner with runtime drains; original server work settles before relay teardown. */
    public synchronized CompletionStage<Void> drain() {
        if(drained!=null)return drained;draining=true;
        drained=server.drain().thenCompose(ignored->{relay.close();return gateway.drain();}).toCompletableFuture().minimalCompletionStage();
        return drained;
    }
    @Override public void close(){drain();}
}
