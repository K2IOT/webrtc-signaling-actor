package io.webrtc.signaling.gateway;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;
/** Bounded pre-TLS concurrent and per-address attempt budgets, independent of AUTH parsing. */
public final class EdgeAdmission {
    public record Limits(int concurrentHandshakes,int trackedIps,int concurrentPerIp,int attemptsPerWindow,Duration window){
        public Limits {if(concurrentHandshakes<1||concurrentHandshakes>1000||trackedIps<concurrentHandshakes||trackedIps>65536||concurrentPerIp<1||concurrentPerIp>concurrentHandshakes||attemptsPerWindow<1||attemptsPerWindow>10000||window==null||window.compareTo(Duration.ofMillis(100))<0||window.compareTo(Duration.ofMinutes(1))>0)throw new IllegalArgumentException("Unsafe edge admission limits");}
        public static Limits candidate(){return new Limits(1000,8192,32,128,Duration.ofSeconds(1));}
    }
    public static final class Rejected extends java.util.concurrent.RejectedExecutionException {public Rejected(){super("EDGE_OVERLOADED");}}
    private static final class State {long windowStart;int attempts,active;State(long now){windowStart=now;}}
    private final Limits limits;private final LongSupplier clock;private final LinkedHashMap<InetAddress,State> ips=new LinkedHashMap<>();private int active;
    public EdgeAdmission(Limits limits,LongSupplier clock){this.limits=Objects.requireNonNull(limits);this.clock=Objects.requireNonNull(clock);}
    public synchronized Ticket acquire(InetAddress address){Objects.requireNonNull(address);long now=clock.getAsLong();var state=ips.get(address);
        if(active>=limits.concurrentHandshakes())throw new Rejected();
        if(state==null){if(ips.size()>=limits.trackedIps()){var iterator=ips.entrySet().iterator();int examined=0;while(iterator.hasNext()&&examined++<128){var item=iterator.next();if(item.getValue().active==0&&now-item.getValue().windowStart>=limits.window().toNanos())iterator.remove();}}if(ips.size()>=limits.trackedIps())throw new Rejected();state=new State(now);ips.put(address,state);}
        if(now-state.windowStart>=limits.window().toNanos()){state.windowStart=now;state.attempts=0;}
        if(state.active>=limits.concurrentPerIp()||state.attempts>=limits.attemptsPerWindow())throw new Rejected();state.attempts++;state.active++;active++;return new Ticket(state);
    }
    public final class Ticket implements AutoCloseable {private final State state;private boolean closed;private Ticket(State state){this.state=state;}public void close(){synchronized(EdgeAdmission.this){if(!closed){closed=true;state.active--;active--;}}}}
    public synchronized int trackedIps(){return ips.size();}public synchronized int activeHandshakes(){return active;}
}
