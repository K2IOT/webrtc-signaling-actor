package io.webrtc.signaling.actors.admission;
import java.util.*;
import java.util.concurrent.*;
/** Credits precede entity enqueue and remain owned through physical work, with reserved safety capacity. */
public final class EntityAdmission {
    public enum Priority {NORMAL,SAFETY}
    public static final class Overloaded extends RejectedExecutionException {public Overloaded(){super("ENTITY_OVERLOADED");}}
    private static final class Usage {int count,bytes;}
    private final int maximumCount,maximumBytes,reservedCount,reservedBytes,processCount,processBytes;private int count,bytes;
    private final Map<String,Usage> entities=new HashMap<>();
    public EntityAdmission(int maximumCount,int maximumBytes,int reservedCount,int reservedBytes,int processCount,int processBytes){if(maximumCount<2||maximumCount>64||maximumBytes<1024||maximumBytes>262144||reservedCount<1||reservedCount>=maximumCount||reservedBytes<1||reservedBytes>=maximumBytes||processCount<maximumCount||processCount>65536||processBytes<maximumBytes||processBytes>268435456)throw new IllegalArgumentException("Unsafe entity admission limits");this.maximumCount=maximumCount;this.maximumBytes=maximumBytes;this.reservedCount=reservedCount;this.reservedBytes=reservedBytes;this.processCount=processCount;this.processBytes=processBytes;}
    public static EntityAdmission forIngressProducers(int maximumActorProcesses){if(maximumActorProcesses<1||maximumActorProcesses>16)throw new IllegalArgumentException("Bounded cluster producer envelope required");return new EntityAdmission(64/maximumActorProcesses,262144/maximumActorProcesses,Math.max(1,16/maximumActorProcesses),Math.max(1,32768/maximumActorProcesses),4096,33554432);}
    public synchronized Ticket acquire(String entity,Priority priority,int encodedBytes){if(entity==null||entity.isBlank()||entity.length()>384||priority==null||encodedBytes<1||encodedBytes>98304)throw new IllegalArgumentException("Invalid admitted entity request");boolean safety=priority==Priority.SAFETY;var usage=entities.get(entity);int n=usage==null?0:usage.count;int b=usage==null?0:usage.bytes;
        if(n>=(safety?maximumCount:maximumCount-reservedCount)||encodedBytes>(safety?maximumBytes:maximumBytes-reservedBytes)-b||count>=(safety?processCount:processCount-reservedCount)||encodedBytes>(safety?processBytes:processBytes-reservedBytes)-bytes)throw new Overloaded();
        if(usage==null){usage=new Usage();entities.put(entity,usage);}usage.count++;usage.bytes+=encodedBytes;count++;bytes+=encodedBytes;return new Ticket(entity,usage,encodedBytes);
    }
    public final class Ticket implements AutoCloseable {private final String entity;private final Usage usage;private final int size;private boolean closed;private Ticket(String entity,Usage usage,int size){this.entity=entity;this.usage=usage;this.size=size;}public void releaseAfter(CompletionStage<?> physical){Objects.requireNonNull(physical).whenComplete((done,failure)->close());}public void close(){synchronized(EntityAdmission.this){if(!closed){closed=true;usage.count--;usage.bytes-=size;count--;bytes-=size;if(usage.count==0)entities.remove(entity,usage);}}}}
    public synchronized int count(String entity){var usage=entities.get(entity);return usage==null?0:usage.count;}public synchronized int retainedBytes(){return bytes;}public synchronized int entities(){return entities.size();}
}
