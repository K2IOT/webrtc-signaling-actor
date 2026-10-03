package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.DbOutcomeUnknownException;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import java.net.*;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSession;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
/** Authenticated direct destination ingress; no caller-supplied peer identity is trusted. */
public final class CellRpcServer implements AutoCloseable {
    public enum Operation {RESERVE,CLAIM,RELEASE,EXECUTE,DELIVER,RELAY,SYNC}
    public record Peer(String cell,String role,String workloadId) {public Peer(String cell,String role){this(cell,role,"");}}
    @FunctionalInterface public interface Backend {CompletionStage<InternalReply> execute(Operation operation,InternalCommand command,Peer peer,Duration remainingBudget);}
    private static final Context.Key<Peer> PEER=Context.key("authenticated-cell-peer");
    private final String cell,environment;private final int requestedPort;private final SslContext tls;private final RpcAdmission admission;private final Backend backend;private final Function<ControlEvent,CompletionStage<InternalReply>> deliver;
    private final ExecutorService executor=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(256),Thread.ofPlatform().daemon().name("cell-rpc-cpu-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private Server server;private NativeSessionHandler sessions;
    public CellRpcServer sessions(NativeSessionHandler handler){if(server!=null)throw new IllegalStateException("Already started");sessions=Objects.requireNonNull(handler);return this;}
    public CellRpcServer(String cell,String environment,int port,SslContext tls,RpcAdmission admission,Backend backend,Function<ControlEvent,CompletionStage<InternalReply>> deliver){if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||environment==null||!environment.matches("[a-z0-9-]{1,32}")||port<0||port>65535)throw new IllegalArgumentException("Invalid RPC listener identity");this.cell=cell;this.environment=environment;requestedPort=port;this.tls=Objects.requireNonNull(tls);this.admission=Objects.requireNonNull(admission);this.backend=Objects.requireNonNull(backend);this.deliver=Objects.requireNonNull(deliver);}
    public CellRpcServer start()throws java.io.IOException {if(server!=null)throw new IllegalStateException("RPC server already started");server=NettyServerBuilder.forPort(requestedPort).sslContext(tls).maxInboundMessageSize(98304).maxConcurrentCallsPerConnection(128).executor(executor).intercept(new ServerInterceptor(){@Override public <Q,A> ServerCall.Listener<Q> interceptCall(ServerCall<Q,A> call,Metadata headers,ServerCallHandler<Q,A> next){Peer peer=extract(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));if(peer==null){call.close(Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED"),new Metadata());return new ServerCall.Listener<>(){};}return Contexts.interceptCall(Context.current().withValue(PEER,peer),call,headers,next);}}).addService(new Ingress()).addService(new SessionService()).build().start();return this;}
    public int port(){if(server==null)throw new IllegalStateException("RPC server not started");return server.getPort();}
    private Peer extract(SSLSession session){return RpcTlsIdentity.extract(session,environment);}
    private static boolean allowed(Peer peer,Operation operation){return peer!=null&&(peer.role().equals("actor")||Set.of(Operation.EXECUTE,Operation.RELAY,Operation.SYNC).contains(operation));}
    private static long maximum(Operation op){return op==Operation.RELAY?1000:2000;}
    private void execute(Operation op,InternalCommand c,StreamObserver<InternalReply> reply){Peer peer=PEER.get();if(!allowed(peer,op)){respond(reply,error(c,"UNAUTHORIZED"));return;}if(!cell.equals(c.getDestinationCell())){respond(reply,error(c,"WRONG_CELL"));return;}
        try{if(c.getSchemaMajor()!=1||c.getSchemaMinor()>1||c.getSchemaMinor()<0||c.getSerializedSize()>98304||c.getRemainingBudgetMs()<=0||c.getOperationId().length()!=36||c.getPayloadHash().size()!=32)throw new IllegalArgumentException("Invalid RPC envelope");UUID.fromString(c.getOperationId());new io.webrtc.signaling.protocol.Identity.CallId(c.getCallId());}
        catch(IllegalArgumentException invalid){respond(reply,error(c,"INVALID_MESSAGE"));return;}
        long remaining=Math.min(c.getRemainingBudgetMs(),maximum(op));Deadline deadline=Context.current().getDeadline();if(deadline==null){respond(reply,error(c,"INVALID_DEADLINE"));return;}remaining=Math.min(remaining,deadline.timeRemaining(TimeUnit.MILLISECONDS));if(remaining<=0){respond(reply,error(c,"OUTCOME_UNKNOWN"));return;}
        RpcAdmission.Ticket ticket;try{ticket=admission.acquire(op==Operation.RELAY?RpcAdmission.Lane.RELAY:RpcAdmission.Lane.CONTROL,c.getSerializedSize());}catch(RpcAdmission.Overloaded e){respond(reply,error(c,"OVERLOADED"));return;}
        try{backend.execute(op,c,peer,Duration.ofMillis(remaining)).whenComplete((value,failure)->{try{if(failure!=null){if(Status.fromThrowable(failure).getCode()==Status.Code.UNAVAILABLE)reply.onError(Status.UNAVAILABLE.asRuntimeException());else respond(reply,error(c,failure instanceof IllegalArgumentException?"INVALID_MESSAGE":"OUTCOME_UNKNOWN"));}else if(!c.getOperationId().equals(value.getOperationId()))respond(reply,error(c,"OUTCOME_UNKNOWN"));else respond(reply,value);}finally{ticket.close();}});}catch(RuntimeException e){ticket.close();respond(reply,error(c,"OUTCOME_UNKNOWN"));}
    }
    private void deliver(ControlEvent c,StreamObserver<InternalReply> reply){if(!allowed(PEER.get(),Operation.DELIVER)){respond(reply,InternalReply.newBuilder().setErrorCode("UNAUTHORIZED").build());return;}var deadline=Context.current().getDeadline();if(deadline==null||deadline.isExpired()||c.getSerializedSize()>98304){respond(reply,InternalReply.newBuilder().setErrorCode("INVALID_MESSAGE").build());return;}RpcAdmission.Ticket ticket;try{ticket=admission.acquire(RpcAdmission.Lane.CONTROL,c.getSerializedSize());}catch(RpcAdmission.Overloaded e){respond(reply,InternalReply.newBuilder().setErrorCode("OVERLOADED").build());return;}try{deliver.apply(c).whenComplete((v,e)->{try{respond(reply,e==null?v:InternalReply.newBuilder().setErrorCode("OUTCOME_UNKNOWN").build());}finally{ticket.close();}});}catch(RuntimeException e){ticket.close();respond(reply,InternalReply.newBuilder().setErrorCode("OUTCOME_UNKNOWN").build());}}
    private static InternalReply error(InternalCommand c,String code){return InternalReply.newBuilder().setOperationId(c.getOperationId()).setStatus("REJECTED").setErrorCode(code).setCallId(c.getCallId()).build();}
    private static void respond(StreamObserver<InternalReply> reply,InternalReply value){reply.onNext(value);reply.onCompleted();}
    private final class SessionService extends SessionIngressGrpc.SessionIngressImplBase {
        @Override public void mutateSession(SessionCommand command,StreamObserver<SessionReply> reply){
            var deadline=Context.current().getDeadline();if(sessions==null||deadline==null||deadline.isExpired()){sessionReply(reply,sessionError(command,"UNAVAILABLE"));return;}
            RpcAdmission.Ticket ticket;try{ticket=admission.acquire(RpcAdmission.Lane.CONTROL,command.getSerializedSize());}catch(RpcAdmission.Overloaded full){sessionReply(reply,sessionError(command,"OVERLOADED"));return;}
            try{var operation=sessions.execute(command,PEER.get(),Duration.ofMillis(Math.max(1,Math.min(2000,deadline.timeRemaining(TimeUnit.MILLISECONDS)))));
                operation.physicalCompletion().whenComplete((done,failure)->ticket.close());
                operation.logical().whenComplete((value,failure)->{if(failure!=null||value==null||!value.getOperationId().equals(command.getOperationId()))sessionReply(reply,sessionError(command,"OUTCOME_UNKNOWN"));else sessionReply(reply,value);});
            }catch(RuntimeException invalid){ticket.close();sessionReply(reply,sessionError(command,"OUTCOME_UNKNOWN"));}
        }
    }
    private static SessionReply sessionError(SessionCommand command,String error){return SessionReply.newBuilder().setOperationId(command.getOperationId()).setStatus("REJECTED").setErrorCode(error).build();}
    private static void sessionReply(StreamObserver<SessionReply> observer,SessionReply reply){observer.onNext(reply);observer.onCompleted();}
    private final class Ingress extends CellIngressGrpc.CellIngressImplBase {
        @Override public void reserveUser(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.RESERVE,c,r);}@Override public void claimAccept(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.CLAIM,c,r);}@Override public void releaseIfCallVersion(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.RELEASE,c,r);}@Override public void executeCallCommand(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.EXECUTE,c,r);}@Override public void relayNegotiation(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.RELAY,c,r);}@Override public void syncCall(InternalCommand c,StreamObserver<InternalReply> r){execute(Operation.SYNC,c,r);}@Override public void deliverControlEvent(ControlEvent c,StreamObserver<InternalReply> r){deliver(c,r);}
    }
    @Override public void close(){if(server!=null)server.shutdown();executor.shutdown();}
}
