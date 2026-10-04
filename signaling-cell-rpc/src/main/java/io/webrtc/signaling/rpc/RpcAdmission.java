package io.webrtc.signaling.rpc;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
/** No borrowing between lanes, before channel creation or task dispatch. */
public final class RpcAdmission {
    public enum Lane {CONTROL,RELAY}
    private static final class Credits {
        final int maximumCount,maximumBytes;int count,bytes;
        Credits(int count,int bytes){if(count<1||bytes<1)throw new IllegalArgumentException("Invalid RPC credits");maximumCount=count;maximumBytes=bytes;}
    }
    private final Credits control,relay;
    private boolean draining;
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    public RpcAdmission(int controlCount,int controlBytes,int relayCount,int relayBytes){control=new Credits(controlCount,controlBytes);relay=new Credits(relayCount,relayBytes);}
    private Credits credits(Lane lane){return lane==Lane.CONTROL?control:relay;}
    public synchronized Ticket acquire(Lane lane,int bytes){
        var credits=credits(lane);
        if(draining||bytes<1||bytes>98304||credits.count>=credits.maximumCount||bytes>credits.maximumBytes-credits.bytes)throw new Overloaded();
        credits.count++;credits.bytes+=bytes;return new Ticket(this,credits,bytes);
    }
    public synchronized int inFlight(Lane lane){return credits(lane).count;}
    public synchronized CompletionStage<Void> drain(){draining=true;if(control.count==0&&relay.count==0)drained.complete(null);return drained.minimalCompletionStage();}
    public static final class Ticket implements AutoCloseable {
        private final RpcAdmission owner;private final Credits credits;private final int bytes;private final AtomicBoolean released=new AtomicBoolean();
        private Ticket(RpcAdmission owner,Credits credits,int bytes){this.owner=owner;this.credits=credits;this.bytes=bytes;}
        @Override public void close(){if(released.compareAndSet(false,true))synchronized(owner){credits.count--;credits.bytes-=bytes;if(owner.draining&&owner.control.count==0&&owner.relay.count==0)owner.drained.complete(null);}}
    }
    public static final class Overloaded extends RuntimeException {public Overloaded(){super("RPC capacity exhausted");}}
}
