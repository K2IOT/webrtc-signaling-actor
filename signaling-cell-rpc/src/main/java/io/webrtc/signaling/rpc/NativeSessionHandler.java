package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
/** Dedicated authenticated session ingress. JWT validation and native policy belong to Operations. */
public final class NativeSessionHandler {
    public record GatewayIdentity(String gatewayId,UUID bootId,String cell,long storageEpoch,String region) {
        public GatewayIdentity {if(gatewayId==null||!gatewayId.matches("[A-Za-z0-9_.-]{1,128}")||bootId==null||cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<=0||region==null||region.isBlank()||region.length()>64)throw new IllegalArgumentException("Invalid gateway identity");}
    }
    public record Request(String type,GatewayIdentity gateway,String token,SessionRepository.Route route,UUID connection,long directoryEpoch,long renewalSequence,UUID operation,@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL) io.webrtc.signaling.protocol.CallCommand proofCommand) {
        public Request(String type,GatewayIdentity gateway,String token,SessionRepository.Route route,UUID connection,long directoryEpoch,long renewalSequence,UUID operation){this(type,gateway,token,route,connection,directoryEpoch,renewalSequence,operation,null);}
        public Request {Objects.requireNonNull(type);Objects.requireNonNull(gateway);Objects.requireNonNull(operation);if(token!=null&&(token.isBlank()||token.length()>16384))throw new IllegalArgumentException("Invalid token");}
        @Override public String toString(){return "SessionRequest[type="+type+", operation="+operation+"]";}
    }
    @FunctionalInterface public interface Operations {RpcOperation<SessionReply> execute(Request request,Duration budget);}
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(81920).maxNumberLength(64).build()).enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build()).findAndRegisterModules().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final String cell;private final long epoch;private final BiPredicate<CellRpcServer.Peer,GatewayIdentity> workloads;private final Operations operations;
    public NativeSessionHandler(String cell,long epoch,BiPredicate<CellRpcServer.Peer,GatewayIdentity> workloads,Operations operations){this.cell=Objects.requireNonNull(cell);if(epoch<=0)throw new IllegalArgumentException("Invalid storage epoch");this.epoch=epoch;this.workloads=Objects.requireNonNull(workloads);this.operations=Objects.requireNonNull(operations);}
    public RpcOperation<SessionReply> execute(SessionCommand command,CellRpcServer.Peer peer,Duration budget){
        try {
            if(peer==null||!peer.role().equals("gateway")||!cell.equals(command.getDestinationCell())||command.getSchemaMajor()!=1||command.getSchemaMinor()<0||command.getSchemaMinor()>1||command.getSerializedSize()>98304||command.getPayload().size()>81920||command.getRemainingBudgetMs()<=0||budget==null||budget.isNegative()||budget.isZero())return rejected(command,"UNAUTHORIZED");
            UUID operation=UUID.fromString(command.getOperationId());var request=JSON.readValue(command.getPayload().toByteArray(),Request.class);var gateway=request.gateway();
            if(!request.operation().equals(operation)||!command.getType().equals(request.type())||!peer.cell().equals(gateway.cell())||peer.workloadId().isBlank()||!peer.workloadId().equals(gateway.gatewayId())||!workloads.test(peer,gateway))return rejected(command,"UNAUTHORIZED");
            if(Set.of("BOOT_START","BOOT_RENEW").contains(request.type())){if(!cell.equals(gateway.cell())||gateway.storageEpoch()!=epoch||request.renewalSequence()<=0||request.token()!=null||request.route()!=null||request.connection()!=null)return rejected(command,"UNAUTHORIZED");}
            else if(request.type().equals("REGISTER")){if(request.token()==null||request.connection()==null||request.route()!=null||request.directoryEpoch()<=0)return rejected(command,"INVALID_MESSAGE");}
            else if(Set.of("REFRESH","CLOSE","READ_PROOF","READ_RELAY_PROOF","READ_INVITE_RESULT").contains(request.type())){var route=request.route();if(route==null||request.directoryEpoch()<=0||!route.gatewayId().equals(gateway.gatewayId())||!route.bootId().equals(gateway.bootId())||Set.of("REFRESH","READ_PROOF","READ_RELAY_PROOF","READ_INVITE_RESULT").contains(request.type())&&request.token()==null)return rejected(command,"UNAUTHORIZED");}
            else return rejected(command,"UNSUPPORTED_OPERATION");
            if(Set.of("READ_PROOF","READ_RELAY_PROOF").contains(request.type())){var proof=request.proofCommand();if(proof==null||proof.callId()==null||!proof.requestId().value().equals(operation)||!proof.sender().userId().equals(request.route().user())||!proof.sender().key().equals(request.route().key())||!proof.sender().incarnation().equals(request.route().incarnation())||proof.sender().connectionGeneration()!=request.route().connectionGeneration()||!proof.sender().connectionId().equals(request.route().connectionId())||(request.type().equals("READ_RELAY_PROOF")? !RelaySessionAuthorizationProof.supports(proof)||!proof.payloadJson().equals("{}"):Set.of(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,io.webrtc.signaling.protocol.SignalEnvelope.Type.ANSWER,io.webrtc.signaling.protocol.SignalEnvelope.Type.ICE_CANDIDATES,io.webrtc.signaling.protocol.SignalEnvelope.Type.END_OF_CANDIDATES).contains(proof.type())))return rejected(command,"UNAUTHORIZED");}
            else if(request.type().equals("READ_INVITE_RESULT")){
                var read=request.proofCommand();var route=request.route();
                if(read==null||read.type()!=io.webrtc.signaling.protocol.SignalEnvelope.Type.GET_COMMAND_RESULT||read.callId()!=null||!read.scope().equals(io.webrtc.signaling.protocol.Identity.CommandScope.invite())||!read.requestId().value().equals(operation)||!read.sender().userId().equals(route.user())||!read.sender().key().equals(route.key())||!read.sender().incarnation().equals(route.incarnation())||read.sender().connectionGeneration()!=route.connectionGeneration()||!read.sender().connectionId().equals(route.connectionId()))return rejected(command,"UNAUTHORIZED");
            }else if(request.proofCommand()!=null)return rejected(command,"INVALID_MESSAGE");
            long millis=Math.min(2000,Math.min(command.getRemainingBudgetMs(),budget.toMillis()));if(millis<=0)return rejected(command,"OUTCOME_UNKNOWN");return operations.execute(request,Duration.ofMillis(millis));
        }catch(Exception invalid){return rejected(command,"UNAUTHORIZED");}
    }
    private static RpcOperation<SessionReply> rejected(SessionCommand command,String code){return new RpcOperation<>(CompletableFuture.completedFuture(SessionReply.newBuilder().setOperationId(command.getOperationId()).setStatus("REJECTED").setErrorCode(code).build()),CompletableFuture.completedFuture(null));}
}
