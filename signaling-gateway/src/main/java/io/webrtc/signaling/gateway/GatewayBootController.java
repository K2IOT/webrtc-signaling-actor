package io.webrtc.signaling.gateway;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.SessionRegistryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;
/** One boot, one pulse scheduler, no resurrection after unknown/expiry. Conservative deadline starts before RPC. */
public final class GatewayBootController implements AutoCloseable {
    @FunctionalInterface public interface Network {CompletionStage<SessionReply> call(NativeSessionHandler.Request request,Duration budget);}
    private enum State {NEW,STARTING,ACTIVE,LOST,CLOSED}
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final NativeSessionHandler.GatewayIdentity identity;private final Network network;private final LongSupplier nanos;
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("gateway-boot-pulse").factory());
    private State state=State.NEW;private boolean pending;private long sequence,deadline;
    public GatewayBootController(NativeSessionHandler.GatewayIdentity identity,Network network,LongSupplier nanos){this.identity=Objects.requireNonNull(identity);this.network=Objects.requireNonNull(network);this.nanos=Objects.requireNonNull(nanos);}
    public static Network network(CellRpcClient client){return (request,budget)->client.session(SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell(request.gateway().cell()).setType(request.type()).setOperationId(request.operation().toString()).setRemainingBudgetMs(Math.max(1,budget.toMillis())).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build(),budget);}
    public NativeSessionHandler.GatewayIdentity identity(){return identity;}
    public synchronized void start(){if(state!=State.NEW)return;state=State.STARTING;send("BOOT_START",1);scheduler.scheduleAtFixedRate(this::pulse,5,5,TimeUnit.SECONDS);}
    public synchronized boolean current(){if(state!=State.ACTIVE)return false;if(nanos.getAsLong()-deadline>=0){state=State.LOST;return false;}return true;}
    public synchronized void pulse(){if(!current()||pending)return;send("BOOT_RENEW",Math.addExact(sequence,1));}
    private void send(String type,long next){pending=true;long started=nanos.getAsLong();var request=new NativeSessionHandler.Request(type,identity,null,null,null,0,next,UUID.randomUUID());
        try{network.call(request,Duration.ofSeconds(2)).toCompletableFuture().orTimeout(2,TimeUnit.SECONDS).whenComplete((reply,failure)->accept(request,started,reply,failure));}catch(RuntimeException failure){pending=false;state=State.LOST;}
    }
    private synchronized void accept(NativeSessionHandler.Request request,long started,SessionReply reply,Throwable failure){
        pending=false;if(state==State.CLOSED||state==State.LOST)return;if(state==State.ACTIVE&&!current())return;
        try{if(failure!=null||reply==null||!reply.getAckCommitted()||!reply.getStatus().equals("COMMITTED")||!reply.getOperationId().equals(request.operation().toString())||reply.getResult().size()>81920)throw new IllegalArgumentException();
            var grant=JSON.readValue(reply.getResult().toByteArray(),SessionRegistryService.BootGrant.class);var boot=grant.boot();
            if(!boot.gatewayId().equals(identity.gatewayId())||!boot.bootId().equals(identity.bootId())||!boot.cell().equals(identity.cell())||boot.storageEpoch()!=identity.storageEpoch()||!boot.region().equals(identity.region())||boot.renewalSequence()!=request.renewalSequence()||!boot.operationId().equals(request.operation())||grant.remainingMillis()<=5000||grant.remainingMillis()>15000)throw new IllegalArgumentException();
            long conservative=started+TimeUnit.MILLISECONDS.toNanos(grant.remainingMillis()-5000);if(nanos.getAsLong()-conservative>=0)throw new IllegalArgumentException();deadline=conservative;sequence=boot.renewalSequence();state=State.ACTIVE;
        }catch(Exception invalid){state=State.LOST;}
    }
    @Override public synchronized void close(){state=State.CLOSED;scheduler.shutdown();}
}
