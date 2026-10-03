package io.webrtc.signaling.gateway;
import io.webrtc.signaling.rpc.DeliveryCreditController;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
/** Bounded per-channel retained writes. Failure/timeout cannot declare control delivered or return in-flight buffers. */
public final class OutboundQueue implements AutoCloseable {
    public enum Kind {CONTROL,RELAY}
    public static final class ResyncRequired extends RejectedExecutionException {public ResyncRequired(){super("RESYNC_REQUIRED");}}
    private static final class Write {final Kind kind;final int bytes;final long since;final Supplier<CompletionStage<Void>> sink;final DeliveryCreditController.Ticket ticket;final CompletableFuture<Void> logical=new CompletableFuture<>();Write(Kind kind,int bytes,long since,Supplier<CompletionStage<Void>> sink,DeliveryCreditController.Ticket ticket){this.kind=kind;this.bytes=bytes;this.since=since;this.sink=sink;this.ticket=ticket;}}
    private final int maximumCount,maximumBytes;private final long maximumAge;private final DeliveryCreditController aggregate;private final LongSupplier clock;private final ArrayDeque<Write> queued=new ArrayDeque<>();private Write running;private int count,bytes;private boolean closed,writable=true;
    public OutboundQueue(int count,int bytes,Duration age,DeliveryCreditController aggregate,LongSupplier clock){if(count<1||count>64||bytes<1||bytes>262144||age==null||age.isNegative()||age.isZero()||age.compareTo(Duration.ofSeconds(10))>0)throw new IllegalArgumentException("Unsafe outbound queue");maximumCount=count;maximumBytes=bytes;maximumAge=age.toNanos();this.aggregate=Objects.requireNonNull(aggregate);this.clock=Objects.requireNonNull(clock);}
    public CompletionStage<Void> offer(Kind kind,byte[] payload,Supplier<CompletionStage<Void>> sink){return offer(kind,payload.length,sink);}
    public synchronized CompletionStage<Void> offer(Kind kind,int retainedBytes,Supplier<CompletionStage<Void>> sink){Objects.requireNonNull(kind);Objects.requireNonNull(sink);if(closed||retainedBytes<1||retainedBytes>98304||count>=maximumCount||retainedBytes>maximumBytes-bytes){close();return CompletableFuture.failedFuture(new ResyncRequired());}
        DeliveryCreditController.Ticket ticket;try{ticket=aggregate.acquire(kind==Kind.CONTROL,retainedBytes);}catch(DeliveryCreditController.Overloaded full){close();return CompletableFuture.failedFuture(new ResyncRequired());}
        var write=new Write(kind,retainedBytes,clock.getAsLong(),sink,ticket);count++;bytes+=retainedBytes;queued.add(write);start();return write.logical.minimalCompletionStage();
    }
    private void start(){if(closed||!writable||running!=null||queued.isEmpty())return;running=queued.remove();var write=running;
        if(clock.getAsLong()-write.since>=maximumAge){release(write);running=null;write.logical.completeExceptionally(new ResyncRequired());close();return;}
        try{write.sink.get().whenComplete((done,error)->finished(write,error));}catch(RuntimeException failure){finished(write,failure);}
    }
    private synchronized void finished(Write write,Throwable failure){if(running!=write)return;release(write);running=null;if(failure==null&&!closed)write.logical.complete(null);else write.logical.completeExceptionally(new ResyncRequired());if(failure!=null)close();else start();}
    private void release(Write write){write.ticket.close();count--;bytes-=write.bytes;}
    public synchronized void writable(boolean writable){this.writable=writable;start();}
    public synchronized void tick(){var oldest=running!=null?running:queued.peek();if(oldest!=null&&clock.getAsLong()-oldest.since>=maximumAge)close();}
    @Override public synchronized void close(){if(closed)return;closed=true;while(!queued.isEmpty()){var write=queued.remove();release(write);write.logical.completeExceptionally(new ResyncRequired());}if(running!=null)running.logical.completeExceptionally(new ResyncRequired());}
    public synchronized boolean closed(){return closed;}public synchronized int retainedBytes(){return bytes;}public synchronized int retainedCount(){return count;}
}
