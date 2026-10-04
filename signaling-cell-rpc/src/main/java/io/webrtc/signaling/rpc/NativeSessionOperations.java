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
    private final SessionAuthorizationProof proofs;private final java.util.function.BooleanSupplier trustedClock;
    private final SessionRegistryService registry;private final BoundedTokenVerifier verifier;private final Clock clock;
    public NativeSessionOperations(SessionRegistryService registry,BoundedTokenVerifier verifier,Clock clock){this(registry,verifier,clock,null,()->false);}
    public NativeSessionOperations(SessionRegistryService registry,BoundedTokenVerifier verifier,Clock clock,SessionAuthorizationProof proofs,java.util.function.BooleanSupplier trustedClock){this.proofs=proofs;this.trustedClock=Objects.requireNonNull(trustedClock);this.registry=Objects.requireNonNull(registry);if(!registry.nativeSecurityConfigured())throw new IllegalArgumentException("Native session security policy required");this.verifier=Objects.requireNonNull(verifier);this.clock=Objects.requireNonNull(clock);}
    @Override public RpcOperation<SessionReply> execute(NativeSessionHandler.Request request,Duration budget){
        var logical=new CompletableFuture<SessionReply>();var physical=new CompletableFuture<Void>();long end=System.nanoTime()+budget.toNanos();
        try {
            switch(request.type()){
                case "BOOT_START"->bridge(request,registry.startGatewayBootTracked(request.gateway().gatewayId(),request.gateway().bootId(),request.gateway().region(),request.operation(),budget),logical,physical);
                case "BOOT_RENEW"->bridge(request,registry.renewGatewayBootTracked(request.gateway().gatewayId(),request.gateway().bootId(),request.renewalSequence(),request.operation(),budget),logical,physical);
                case "CLOSE"->bridge(request,registry.closeSessionIfGenerationTracked(request.route(),request.directoryEpoch(),budget),logical,physical);
                case "REGISTER","REFRESH","READ_PROOF"->verifier.verify(request.token(),clock.instant()).whenComplete((principal,failure)->{
                    if(failure!=null){logical.complete(error(request,"UNAUTHORIZED"));physical.complete(null);return;}
                    long left=end-System.nanoTime();if(left<=0){logical.complete(error(request,"OUTCOME_UNKNOWN"));physical.complete(null);return;}
                    try {if(request.type().equals("READ_PROOF")){
                        if(proofs==null||!trustedClock.getAsBoolean())throw new AuthoritySql.FencedException();
                        var read=registry.readCurrentSessionTracked(request.route(),principal,request.directoryEpoch(),Duration.ofNanos(left));
                        read.physicalCompletion().whenComplete((done,error)->physical.complete(null));
                        read.logical().whenComplete((view,error)->{if(error!=null){logical.complete(error(request,"UNAUTHORIZED"));return;}try{if(!trustedClock.getAsBoolean()||!view.proofUntil().isAfter(clock.instant()))throw new AuthoritySql.FencedException();String sealed=proofs.issue(view,request.proofCommand());logical.complete(SessionReply.newBuilder().setOperationId(request.operation().toString()).setStatus("READ").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(sealed))).build());}catch(RuntimeException invalid){logical.complete(error(request,"UNAUTHORIZED"));}});
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
    private static <T> void bridge(NativeSessionHandler.Request request,DbOperation<T> operation,CompletableFuture<SessionReply> logical,CompletableFuture<Void> physical){
        operation.physicalCompletion().whenComplete((done,failure)->physical.complete(null));
        operation.logical().whenComplete((value,failure)->{if(failure!=null){logical.complete(error(request,"OUTCOME_UNKNOWN"));return;}try{logical.complete(SessionReply.newBuilder().setOperationId(request.operation().toString()).setAckCommitted(true).setStatus("COMMITTED").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(value))).build());}catch(RuntimeException invalid){logical.complete(error(request,"OUTCOME_UNKNOWN"));}});
    }
    private static SessionReply error(NativeSessionHandler.Request request,String error){return SessionReply.newBuilder().setOperationId(request.operation().toString()).setStatus("REJECTED").setErrorCode(error).build();}
}
