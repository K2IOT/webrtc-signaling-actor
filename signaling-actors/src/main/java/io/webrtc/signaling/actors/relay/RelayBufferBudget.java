package io.webrtc.signaling.actors.relay;
/** Shared payload/replay retention bound; no per-idle-call allocation reservation. */
public final class RelayBufferBudget {
    public static final RelayBufferBudget PROCESS=new RelayBufferBudget(268435456);
    private final long maximum;private final int maximumTickets;private int tickets;private long retained;
    public RelayBufferBudget(long maximum){this(maximum,65536);}
    public RelayBufferBudget(long maximum,int maximumTickets){if(maximum<1||maximum>268435456||maximumTickets<1||maximumTickets>65536)throw new IllegalArgumentException("Unsafe relay memory budget");this.maximum=maximum;this.maximumTickets=maximumTickets;}
    public synchronized Ticket acquire(int bytes){if(bytes<1||bytes>maximum-retained||tickets>=maximumTickets)throw new Overloaded();retained+=bytes;tickets++;return new Ticket(bytes);}
    public final class Ticket implements AutoCloseable {private final int bytes;private boolean closed;private Ticket(int bytes){this.bytes=bytes;}public void close(){synchronized(RelayBufferBudget.this){if(!closed){closed=true;retained-=bytes;tickets--;}}}}
    public synchronized long retainedBytes(){return retained;}
    public static final class Overloaded extends java.util.concurrent.RejectedExecutionException {public Overloaded(){super("RELAY_OVERLOADED");}}
}
