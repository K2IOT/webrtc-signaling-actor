package io.webrtc.signaling.gateway;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/** Original JWT -> native home auth read -> destination command, within one monotonic budget. */
public final class NativeGatewayCommands implements NativeGatewayServices.ContextCommands {
    public interface Network {
        CompletionStage<SessionReply> session(SessionCommand command,Duration budget);
        CompletionStage<InternalReply> call(CellRpcServer.Operation operation,InternalCommand command,Duration budget);
    }
    public interface TrackedNetwork extends Network {
        RpcOperation<SessionReply> sessionTracked(SessionCommand command,Duration budget);
        default CompletionStage<SessionReply> session(SessionCommand command,Duration budget){return sessionTracked(command,budget).logical();}
    }
    public static TrackedNetwork network(CellRpcClient client){Objects.requireNonNull(client);return new TrackedNetwork(){
        public RpcOperation<SessionReply> sessionTracked(SessionCommand c,Duration b){return client.sessionTracked(c,b);}
        public CompletionStage<InternalReply> call(CellRpcServer.Operation op,InternalCommand c,Duration b){return client.call(op,c,b);}
    };}
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final NativeSessionHandler.GatewayIdentity gateway;
    private final Function<UserId,ProofBindings.TrustedHome> homes;
    private final Network network;
    private final Clock clock;private final long routingEpoch;private volatile NativeRelaySessionProofCache relayCache;
    public synchronized NativeGatewayCommands relayProofCache(NativeRelaySessionProofCache cache){if(!(network instanceof TrackedNetwork)||relayCache!=null)throw new IllegalStateException("Original tracked session network and one-time installation required");relayCache=Objects.requireNonNull(cache);return this;}
    public NativeGatewayCommands(NativeSessionHandler.GatewayIdentity gateway,Function<UserId,ProofBindings.TrustedHome> homes,Network network,Clock clock){this(gateway,homes,network,clock,gateway.storageEpoch());}
    public NativeGatewayCommands(NativeSessionHandler.GatewayIdentity gateway,Function<UserId,ProofBindings.TrustedHome> homes,Network network,Clock clock,long routingEpoch){if(routingEpoch<1)throw new IllegalArgumentException("Invalid routing epoch");this.routingEpoch=routingEpoch;this.gateway=Objects.requireNonNull(gateway);this.homes=Objects.requireNonNull(homes);this.network=Objects.requireNonNull(network);this.clock=Objects.requireNonNull(clock);}
    @Override public CompletionStage<String> execute(CallCommand original,SessionRepository.Route route,String token,Duration budget){
        try{
            if(budget==null||budget.isNegative()||budget.isZero())return CompletableFuture.completedFuture(error(original,"OUTCOME_UNKNOWN"));
            var home=Objects.requireNonNull(homes.apply(original.sender().userId()));
            if(!home.cell().equals(gateway.cell())||home.storageEpoch()!=gateway.storageEpoch()||route==null||token==null||token.isBlank()||!route.gatewayId().equals(gateway.gatewayId())||!route.bootId().equals(gateway.bootId())||!same(route,original.sender()))return CompletableFuture.completedFuture(error(original,"UNAUTHORIZED"));
            boolean invite=original.type()==SignalEnvelope.Type.INVITE;
            boolean lookup=original.type()==SignalEnvelope.Type.GET_COMMAND_RESULT&&original.callId()==null;
            if((invite||lookup)&&!original.scope().equals(CommandScope.invite()))return CompletableFuture.completedFuture(error(original,"INVALID_MESSAGE"));
            var call=invite?CallId.create(home.cell(),routingEpoch):original.callId();
            if(!lookup&&call==null)return CompletableFuture.completedFuture(error(original,"INVALID_MESSAGE"));
            var command=invite?new CallCommand(original.type(),original.sender(),original.requestId(),call,original.scope(),original.target(),original.negotiationId(),original.iceGeneration(),original.payloadJson(),original.intentHash()):original;
            boolean relay=Set.of(SignalEnvelope.Type.OFFER,SignalEnvelope.Type.ANSWER,SignalEnvelope.Type.ICE_CANDIDATES,SignalEnvelope.Type.END_OF_CANDIDATES).contains(command.type());
            long end=System.nanoTime()+Math.min(budget.toNanos(),Duration.ofSeconds(relay?1:2).toNanos());
            if(relay&&!RelaySessionAuthorizationProof.supports(command))return CompletableFuture.completedFuture(error(original,"INVALID_MESSAGE"));
            var proofCommand=relay?new CallCommand(command.type(),command.sender(),command.requestId(),command.callId(),command.scope(),null,command.negotiationId(),command.iceGeneration(),"{}",command.intentHash()):command;
            var request=new NativeSessionHandler.Request(lookup?"READ_INVITE_RESULT":relay?"READ_RELAY_PROOF":"READ_PROOF",gateway,token,route,null,home.directoryEpoch(),0,command.requestId().value(),proofCommand);
            var session=SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell(home.cell()).setOperationId(request.operation().toString()).setType(request.type()).setRemainingBudgetMs(Math.max(1,remaining(end).toMillis())).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
            return readSession(session,proofCommand,home,relay,end).thenCompose(reply->{
                if(!reply.getOperationId().equals(session.getOperationId())||reply.getAckCommitted()||!reply.getErrorCode().isEmpty()||!reply.getStatus().equals("READ"))return CompletableFuture.completedFuture(error(original,reply.getErrorCode().isEmpty()?"OUTCOME_UNKNOWN":reply.getErrorCode()));
                try{
                    if(lookup){var value=JSON.readTree(reply.getResult().toByteArray());return CompletableFuture.completedFuture(value.isNull()?error(original,"RESULT_EXPIRED"):outcome(original,JSON.treeToValue(value,CallCommandService.Outcome.class),false));}
                    String proof=JSON.readValue(reply.getResult().toByteArray(),String.class);
                    Object payload=command.type()==SignalEnvelope.Type.SYNC_CALL?new RpcBusinessHandler.SyncRead(command.sender(),command.callId(),command.requestId(),proof,command.intentHash()):new RpcBusinessHandler.CallPayload(command,proof);
                    var wire=InternalCommand.newBuilder().setSchemaMajor(1).setOperationId(command.requestId().value().toString()).setType(command.type().name()).setSender(sender(command.sender())).setCallId(call.value()).setCommandScope(command.scope().value()).setPayloadHash(ByteString.copyFrom(HexFormat.of().parseHex(command.intentHash()))).setDestinationCell(call.coordinatorCell()).setRemainingBudgetMs(Math.max(1,remaining(end).toMillis())).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(payload))).build();
                    var operation=switch(command.type()){
                        case OFFER,ANSWER,ICE_CANDIDATES,END_OF_CANDIDATES->CellRpcServer.Operation.RELAY;
                        case SYNC_CALL->CellRpcServer.Operation.SYNC;
                        default->CellRpcServer.Operation.EXECUTE;
                    };
                    return network.call(operation,wire,remaining(end)).thenApply(result->{
                        if(operation==CellRpcServer.Operation.RELAY&&result.getAckCommitted())return error(original,"OUTCOME_UNKNOWN");
                        if(!result.getOperationId().equals(wire.getOperationId())||!result.getCallId().equals(wire.getCallId())||!result.getErrorCode().isEmpty()&&!(result.getStatus().equals("PENDING")&&result.getErrorCode().equals("WORKFLOW_PENDING")))return error(original,result.getErrorCode().isEmpty()?"OUTCOME_UNKNOWN":result.getErrorCode());
                        try{
                            if(command.type()==SignalEnvelope.Type.SYNC_CALL){if(result.getAckCommitted())return error(original,"OUTCOME_UNKNOWN");return snapshot(original,JSON.readValue(result.getResult().toByteArray(),CallSnapshotRepository.Snapshot.class));}
                            if(command.type()==SignalEnvelope.Type.GET_COMMAND_RESULT){
                                if(result.getAckCommitted()||!result.getStatus().equals("READ"))return error(original,"OUTCOME_UNKNOWN");
                                var read=JSON.readTree(result.getResult().toByteArray());return read.isNull()?error(original,"RESULT_EXPIRED"):outcome(original,JSON.treeToValue(read,CallCommandService.Outcome.class),false);
                            }
                            var value=JSON.readValue(result.getResult().toByteArray(),CallCommandService.Outcome.class);
                            if(!result.getAckCommitted()&&result.getStatus().equals("PENDING")&&value.status().equals("PENDING")&&value.code().equals("WORKFLOW_PENDING"))return outcome(original,value,false);
                            if(!result.getAckCommitted()||!result.getStatus().equals("COMMITTED")||!value.status().equals("FINAL"))return error(original,"OUTCOME_UNKNOWN");
                            return outcome(original,value,true);
                        }catch(Exception invalid){return error(original,"OUTCOME_UNKNOWN");}
                    });
                }catch(Exception invalid){return CompletableFuture.completedFuture(error(original,"OUTCOME_UNKNOWN"));}
            }).exceptionally(failure->error(original,"OUTCOME_UNKNOWN"));
        }catch(RuntimeException invalid){return CompletableFuture.completedFuture(error(original,"OUTCOME_UNKNOWN"));}
    }
    private CompletionStage<SessionReply> readSession(SessionCommand session,CallCommand command,ProofBindings.TrustedHome home,boolean relay,long end){
        var cache=relayCache;if(!relay||cache==null)return network.session(session,remaining(end));
        var work=cache.get(command,home,remaining(end),()->{
            var original=((TrackedNetwork)network).sessionTracked(session,remaining(end));
            var logical=original.logical().thenApply(reply->{
                if(!reply.getOperationId().equals(session.getOperationId())||reply.getAckCommitted()||!reply.getErrorCode().isEmpty()||!reply.getStatus().equals("READ"))throw new CompletionException(new IllegalStateException("Native relay proof unavailable"));
                try{return JSON.readValue(reply.getResult().toByteArray(),String.class);}catch(java.io.IOException invalid){throw new CompletionException(invalid);}
            });
            return new RpcOperation<>(logical,original.physicalCompletion());
        });
        // Cached proof keeps its signed original native checkedAt/proofUntil; READ is internal correlation only.
        return work.logical().thenApply(proof->SessionReply.newBuilder().setOperationId(session.getOperationId()).setStatus("READ").setResult(ByteString.copyFrom(RpcBusinessHandler.encode(proof))).build());
    }
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
    private static boolean same(SessionRepository.Route r,AuthenticatedSession s){return r.user().equals(s.userId())&&r.key().equals(s.key())&&r.incarnation().equals(s.incarnation())&&r.connectionGeneration()==s.connectionGeneration()&&r.connectionId().equals(s.connectionId());}
    private static ByteString uuid(UUID id){return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());}
    private static SessionIdentity sender(AuthenticatedSession s){return SessionIdentity.newBuilder().setIssuer(s.key().issuer()).setJti(s.key().jti()).setUserId(s.userId().value()).setIncarnation(uuid(s.incarnation().value())).setConnectionGeneration(s.connectionGeneration()).setConnectionId(uuid(s.connectionId())).build();}
    private ObjectNode envelope(CallCommand c,String type){return JSON.createObjectNode().put("v",1).put("type",type).put("requestId",c.requestId().value().toString()).put("sessionIncarnation",c.sender().incarnation().value().toString()).put("connectionGeneration",Long.toString(c.sender().connectionGeneration())).put("serverTime",clock.instant().toString());}
    private String error(CallCommand c,String code){var root=envelope(c,"ERROR").put("ackCommitted",false);root.putObject("error").put("code",code);return root.toString();}
    private String outcome(CallCommand c,CallCommandService.Outcome result,boolean committed){var root=envelope(c,committed?"ACK_COMMITTED":"COMMAND_RESULT").put("ackCommitted",committed).put("callVersion",Long.toString(result.version()));if(result.callId()!=null)root.put("callId",result.callId().value());var value=root.putObject("result").put("status",result.status()).put("code",result.code()).put("state",result.state());var events=value.putArray("eventIds");result.eventIds().forEach(id->events.add(id.toString()));return root.toString();}
    private String snapshot(CallCommand c,CallSnapshotRepository.Snapshot s){if(!s.callId().equals(c.callId()))return error(c,"OUTCOME_UNKNOWN");var root=envelope(c,"CALL_SNAPSHOT").put("ackCommitted",false).put("callId",s.callId().value()).put("callVersion",Long.toString(s.version())).put("negotiationId",Long.toString(s.negotiationId()));var result=root.putObject("result");NativeCallSnapshot.write(result,s);root.put("iceGeneration",result.path("negotiation").path("iceGeneration").asText());return root.toString();}
}
