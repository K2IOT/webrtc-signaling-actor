package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.protocol.internal.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Gateway process ingress: original volatile write receipts, authenticated coordinator, boot-bound target. */
public final class GatewayRelayRpcServer implements AutoCloseable {
    @FunctionalInterface public interface Backend {RpcOperation<RelayWriteReceipt> send(RelayDelivery delivery,CellRpcServer.Peer peer,Duration budget);}
    private static final Context.Key<CellRpcServer.Peer> PEER=Context.key("gateway-relay-authenticated-peer");
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(81920).build()).build()).findAndRegisterModules().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final String cell,gateway,environment;private final UUID boot;private final int requestedPort;private final SslContext tls;private final RpcAdmission admission;private final Backend backend;
    private final ExecutorService cpu=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(256),Thread.ofPlatform().daemon().name("gateway-relay-rpc-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private final CompletableFuture<Void> drained=new CompletableFuture<>();private Server server;private volatile boolean draining;
    public GatewayRelayRpcServer(String cell,String gatewayId,UUID bootId,String environment,int port,SslContext tls,RpcAdmission admission,Backend backend){
        if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||gatewayId==null||!gatewayId.matches("[A-Za-z0-9_.-]{1,128}")||bootId==null||environment==null||!environment.matches("[a-z0-9-]{1,32}")||port<0||port>65535)throw new IllegalArgumentException("Invalid gateway RPC identity");
        this.cell=cell;gateway=gatewayId;boot=bootId;this.environment=environment;requestedPort=port;this.tls=Objects.requireNonNull(tls);this.admission=Objects.requireNonNull(admission);this.backend=Objects.requireNonNull(backend);
    }
    public synchronized GatewayRelayRpcServer start()throws java.io.IOException {
        if(draining||server!=null)throw new IllegalStateException("Gateway RPC unavailable");
        server=NettyServerBuilder.forPort(requestedPort).sslContext(tls).maxInboundMessageSize(98304).maxConcurrentCallsPerConnection(128).executor(cpu).intercept(new ServerInterceptor(){
            @Override public <Q,A> ServerCall.Listener<Q> interceptCall(ServerCall<Q,A> call,Metadata headers,ServerCallHandler<Q,A> next){
                var peer=RpcTlsIdentity.extract(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION),environment);
                if(peer==null||!peer.role().equals("actor")||peer.workloadId().isBlank()){call.close(Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED"),new Metadata());return new ServerCall.Listener<>(){};}
                return Contexts.interceptCall(Context.current().withValue(PEER,peer),call,headers,next);
            }
        }).addService(new GatewayRelayIngressGrpc.GatewayRelayIngressImplBase(){@Override public void deliverRelay(GatewayRelayRequest request,StreamObserver<InternalReply> reply){deliver(request,reply);}}).build().start();return this;
    }
    public synchronized int port(){if(server==null)throw new IllegalStateException("Gateway RPC not started");return server.getPort();}
    private void deliver(GatewayRelayRequest request,StreamObserver<InternalReply> reply){
        final long end;final RpcAdmission.Ticket ticket;
        try{
            var deadline=Context.current().getDeadline();
            if(draining||request.getSchemaMajor()!=1||!cell.equals(request.getDestinationCell())||!gateway.equals(request.getGatewayId())||!boot.equals(RpcBusinessHandler.uuid(request.getBootId()))||request.getSerializedSize()>98304||request.getPayload().isEmpty()||request.getPayload().size()>81920||request.getRemainingBudgetMs()<=0||deadline==null)throw new IllegalArgumentException();
            UUID.fromString(request.getOperationId());long remaining=Math.min(1000,Math.min(request.getRemainingBudgetMs(),deadline.timeRemaining(TimeUnit.MILLISECONDS)));if(remaining<=0)throw new IllegalArgumentException();end=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(remaining);
            ticket=admission.acquire(RpcAdmission.Lane.RELAY,request.getSerializedSize());
        }catch(RpcAdmission.Overloaded full){respond(reply,error(request,"OVERLOADED"));return;}
        catch(RuntimeException invalid){respond(reply,error(request,"INVALID_MESSAGE"));return;}
        final RelayDelivery delivery;
        try{
            delivery=JSON.readValue(request.getPayload().toByteArray(),RelayDelivery.class);
            if(!request.getOperationId().equals(delivery.command().requestId().value().toString())||!PEER.get().cell().equals(delivery.command().callId().coordinatorCell()))throw new IllegalArgumentException();
        }catch(Exception invalid){ticket.close();respond(reply,error(request,"INVALID_MESSAGE"));return;}
        try{
            long remaining=end-System.nanoTime();if(remaining<=0){ticket.close();respond(reply,error(request,"RESYNC_REQUIRED"));return;}
            var operation=backend.send(delivery,PEER.get(),Duration.ofNanos(remaining));var replied=new CompletableFuture<Void>();
            operation.physicalCompletion().thenCombine(replied,(a,b)->null).thenRun(ticket::close);
            operation.logical().thenApply(value->value).toCompletableFuture().orTimeout(Math.max(1,end-System.nanoTime()),TimeUnit.NANOSECONDS).whenComplete((receipt,failure)->{
                try{
                    if(failure!=null||receipt==null||!receipt.matches(delivery.command())||receipt.callVersion()!=delivery.callVersion())respond(reply,error(request,"RESYNC_REQUIRED"));
                    else respond(reply,InternalReply.newBuilder().setOperationId(request.getOperationId()).setCallId(receipt.call().value()).setCallVersion(receipt.callVersion()).setStatus("WRITE_COMPLETED").setResult(com.google.protobuf.ByteString.copyFrom(RpcBusinessHandler.encode(receipt))).build());
                }finally{replied.complete(null);}
            });
        }catch(Throwable unknown){
            // A throwing producer cannot prove it never started a native write. Retain this admission.
            respond(reply,error(request,"RESYNC_REQUIRED"));
        }
    }
    private static InternalReply error(GatewayRelayRequest request,String code){return InternalReply.newBuilder().setOperationId(request.getOperationId()).setStatus("REJECTED").setErrorCode(code).build();}
    private static void respond(StreamObserver<InternalReply> observer,InternalReply reply){observer.onNext(reply);observer.onCompleted();}
    public synchronized CompletionStage<Void> drain(){
        if(!draining){draining=true;if(server!=null)server.shutdown();admission.drain().whenComplete((v,failure)->{
            if(failure!=null){drained.completeExceptionally(failure);return;}if(server==null){cpu.shutdown();drained.complete(null);return;}
            Thread.startVirtualThread(()->{try{if(!server.awaitTermination(2,TimeUnit.SECONDS))throw new TimeoutException("Gateway RPC transport cleanup unknown");cpu.shutdown();drained.complete(null);}catch(Exception unknown){if(unknown instanceof InterruptedException)Thread.currentThread().interrupt();drained.completeExceptionally(unknown);}});
        });}
        return drained.minimalCompletionStage();
    }
    public CompletionStage<Void> settleAdmitted(){return admission.settleAdmitted();}
    @Override public void close(){drain();}
}
