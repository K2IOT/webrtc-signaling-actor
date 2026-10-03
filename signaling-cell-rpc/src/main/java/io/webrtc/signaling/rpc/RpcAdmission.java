package io.webrtc.signaling.rpc;
import java.util.concurrent.atomic.AtomicBoolean;
/** No borrowing between lanes, before channel creation or task dispatch. */
public final class RpcAdmission {
    public enum Lane {CONTROL,RELAY}
    private static final class Credits {final int maximumCount,maximumBytes;int count,bytes;Credits(int count,int bytes){if(count<1||bytes<1)throw new IllegalArgumentException("Invalid RPC credits");maximumCount=count;maximumBytes=bytes;}}
    private final Credits control,relay;
    public RpcAdmission(int controlCount,int controlBytes,int relayCount,int relayBytes){control=new Credits(controlCount,controlBytes);relay=new Credits(relayCount,relayBytes);}
    private Credits credits(Lane lane){return lane==Lane.CONTROL?control:relay;}
    public Ticket acquire(Lane lane,int bytes){var credits=credits(lane);synchronized(credits){if(bytes<1||bytes>98304||credits.count>=credits.maximumCount||bytes>credits.maximumBytes-credits.bytes)throw new Overloaded();credits.count++;credits.bytes+=bytes;return new Ticket(credits,bytes);}}
    public int inFlight(Lane lane){var credits=credits(lane);synchronized(credits){return credits.count;}}
    public static final class Ticket implements AutoCloseable {private final Credits credits;private final int bytes;private final AtomicBoolean released=new AtomicBoolean();private Ticket(Credits credits,int bytes){this.credits=credits;this.bytes=bytes;}@Override public void close(){if(released.compareAndSet(false,true))synchronized(credits){credits.count--;credits.bytes-=bytes;}}}
    public static final class Overloaded extends RuntimeException {public Overloaded(){super("RPC capacity exhausted");}}
}
