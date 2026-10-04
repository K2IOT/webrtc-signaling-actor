package io.webrtc.signaling.rpc;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.internal.SessionReply;
import io.webrtc.signaling.storage.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
/** Native home-store operations with independent home JWT verification and in-transaction revocation policy. */
public final class NativeSessionOperations implements NativeSessionHandler.Operations {
    private final CallAuthorizationPolicy callPolicy;
    private final SessionAuthorizationProof proofs;private final java.util.function.BooleanSupplier trustedClock;
    private final SessionRegistryService registry;private final BoundedTokenVerifier verifier;private final Clock clock;
    public NativeSessionOperations(SessionRegistryService registry,BoundedTokenVerifier verifier,Clock clock){this(registry,verifier,clock,null,()->false);}
    public NativeSessionOperations(SessionRegistryService registry,BoundedTokenVerifier verifier,Clock clock,SessionAuthorizationProof proofs,java.util.function.BooleanSupplier trustedClock){this(registry,verifier,clock,proofs,trustedClock,CallAuthorizationPolicy.denyAll());}
    public NativeSessionOperations(SessionRegistryService registry,BoundedTokenVerifier verifier,Clock clock,SessionAuthorizationProof proofs,java.util.function.BooleanSupplier trustedClock,CallAuthorizationPolicy callPolicy){this.callPolicy=Objects.requireNonNull(callPolicy);this.proofs=proofs;this.trustedClock=Objects.requireNonNull(trustedClock);this.registry=Objects.requireNonNull(registry);if(!registry.nativeSecurityConfigured())throw new IllegalArgumentException("Native session security policy required");this.verifier=Objects.requireNonNull(verifier);this.clock=Objects.requireNonNull(clock);}
    @Override public RpcOperation<SessionReply> execute(NativeSessionHandler.Request request,Duration budget){
        var logical=new CompletableFuture<SessionReply>();var physical=new CompletableFuture<Void>();long end=System.nanoTime()+budget.toNanos();
        try {
            switch(request.type()){
                case "BOOT_START"->bridge(request,registry.startGatewayBootTracked(request.gateway().gatewayId(),request.gateway().bootId(),request.gateway().region(),request.operation(),budget),logical,physical);
                case "BOOT_RENEW"->bridge(request,registry.renewGatewayBootTracked(request.gateway().gatewayId(),request.gateway().bootId(),request.renewalSequence(),request.operation(),budget),logical,physical);
                case "CLOSE"->bridge(request,registry.closeSessionIfGenerationTracked(request.route(),request.directoryEpoch(),budget),logical,physical);
                case "REGISTER","REFRESH","READ_PROOF","READ_INVITE_RESULT"->prepare(request).whenComplete((prepared,failure)->{
                    var principal=prepared==null?null:prepared.principal();
                    if(failure!=null){logical.complete(error(request,forbidden(failure)?"FORBIDDEN":"UNAUTHORIZED"));physical.complete(null);return;}
                    long left=end-System.nanoTime();if(left<=0){logical.complete(error(request,"OUTCOME_UNKNOWN"));physical.complete(null);return;}
                    try {if(request.type().equals("READ_INVITE_RESULT")){
                        readReply(request,registry.readInviteResultTracked(request.route(),principal,request.directoryEpoch(),request.proofCommand().requestId(),Duration.ofNanos(left)),logical,physical);
                    }else if(request.type().equals("READ_PROOF")){
                        if(proofs==null||!trustedClock.getAsBoolean())throw new AuthoritySql.FencedException();
                        var read=registry.readCurrentSessionTracked(request.route(),principal,request.directoryEpoch(),Duration.ofNanos(left));
                        read.physicalCompletion().whenComplete((done,error)->physical.complete(null));
                        read.logical().whenComplete((view,error)->{if(error!=null){logical.complete(error(request,"UNAUTHORIZED"));return;}try{if(!trustedClock.getAsBoolean()||!view.proofUntil().isAfter(clock.instant()))throw new AuthoritySql.FencedException();Instant until=view.proofUntil().isBefore(prepared.permissionUntil())?view.proofUntil():prepared.permissionUntil();if(!until.isAfter(clock.instant()))throw new AuthoritySql.FencedException();
                            var capped=new SessionRegistryService.SessionProofView(view.route(),view.checkedAt(),until,view.sourceCell(),view.sourceStorageEpoch(),view.directoryEpoch());String sealed=proofs.issue(capped,request.proofCommand());logical.complete(SessionReply.newBuilder().setOperationId(request.operation().toString()).setStatus("READ").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(sealed))).build());}catch(RuntimeException invalid){logical.complete(error(request,"UNAUTHORIZED"));}});
                    }else if(request.type().equals("REGISTER")){
                        var identity=request.gateway();var boot=new GatewayLeaseRepository.Boot(identity.gatewayId(),identity.bootId(),identity.storageEpoch(),identity.region(),identity.cell(),0,Instant.EPOCH,request.operation());
                        bridge(request,registry.registerSessionTracked(principal,boot,request.connection(),request.directoryEpoch(),Duration.ofNanos(left)),logical,physical);
                    }else bridge(request,registry.refreshSessionTracked(request.route(),principal,request.directoryEpoch(),Duration.ofNanos(left)),logical,physical);
                    }catch(RuntimeException invalid){logical.complete(error(request,"OUTCOME_UNKNOWN"));physical.complete(null);}
                });
                default->{logical.complete(error(request,"UNSUPPORTED_OPERATION"));physical.complete(null);}
            }
        }catch(RuntimeException invalid){logical.complete(error(request,"OUTCOME_UNKNOWN"));physical.complete(null);}
        return new RpcOperation<>(logical.minimalCompletionStage(),physical.minimalCompletionStage());
    }
    private record Prepared(AuthPrincipal principal,Instant permissionUntil) {}
    private static final class Forbidden extends RuntimeException {}
    private static boolean forbidden(Throwable failure){while(failure instanceof CompletionException&&failure.getCause()!=null)failure=failure.getCause();return failure instanceof Forbidden;}
    private CompletionStage<Prepared> prepare(NativeSessionHandler.Request request){
        return verifier.verify(request.token(),clock.instant()).thenCompose(principal->{
            if(!request.type().equals("READ_PROOF")||request.proofCommand()==null||request.proofCommand().type()!=io.webrtc.signaling.protocol.SignalEnvelope.Type.INVITE)return CompletableFuture.completedFuture(new Prepared(principal,Instant.MAX));
            var target=request.proofCommand().target();if(target==null||target.equals(principal.userId()))return CompletableFuture.failedFuture(new Forbidden());
            return CallAuthorizationPolicy.guarded(callPolicy::authorize).authorize(new CallAuthorizationRequest(principal,target,clock.instant())).thenApply(decision->{if(!decision.allowed()||!decision.expiresAt().isAfter(clock.instant()))throw new Forbidden();return new Prepared(principal,decision.expiresAt());});
        });
    }
    private static <T> void readReply(NativeSessionHandler.Request request,DbOperation<T> operation,CompletableFuture<SessionReply> logical,CompletableFuture<Void> physical){
        operation.physicalCompletion().whenComplete((done,failure)->physical.complete(null));
        operation.logical().whenComplete((value,failure)->{if(failure!=null){logical.complete(error(request,"UNAUTHORIZED"));return;}try{logical.complete(SessionReply.newBuilder().setOperationId(request.operation().toString()).setStatus("READ").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(value))).build());}catch(RuntimeException invalid){logical.complete(error(request,"OUTCOME_UNKNOWN"));}});
    }
    private static <T> void bridge(NativeSessionHandler.Request request,DbOperation<T> operation,CompletableFuture<SessionReply> logical,CompletableFuture<Void> physical){
        operation.physicalCompletion().whenComplete((done,failure)->physical.complete(null));
        operation.logical().whenComplete((value,failure)->{if(failure!=null){logical.complete(error(request,"OUTCOME_UNKNOWN"));return;}try{logical.complete(SessionReply.newBuilder().setOperationId(request.operation().toString()).setAckCommitted(true).setStatus("COMMITTED").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(value))).build());}catch(RuntimeException invalid){logical.complete(error(request,"OUTCOME_UNKNOWN"));}});
    }
    private static SessionReply error(NativeSessionHandler.Request request,String error){return SessionReply.newBuilder().setOperationId(request.operation().toString()).setStatus("REJECTED").setErrorCode(error).build();}
}
