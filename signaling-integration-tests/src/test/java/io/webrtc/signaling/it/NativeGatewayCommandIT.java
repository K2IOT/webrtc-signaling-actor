package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.actors.admission.TrackedEntityAsk;
import io.webrtc.signaling.actors.cluster.CallMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.ActorRef;
import java.io.File;
import java.time.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class NativeGatewayCommandIT {
    static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    static File cert(String name){return new File(Objects.requireNonNull(NativeGatewayCommandIT.class.getResource("/test-only-pki/session/"+name)).getFile());}
    @Test void gatewayObtainsNativeAuthProofAndOriginalInviteReplaysAfterGroupRelease()throws Exception{runControl(false);}
    @Test void nativeRemotePreparationStaysPendingWithoutManufacturingCommitAcknowledgment()throws Exception{runControl(true);}
    static com.fasterxml.jackson.databind.JsonNode retryTransient(NativeGatewayServices gateway,CallCommand command,SessionRepository.Route route)throws Exception {
        long end=System.nanoTime()+Duration.ofSeconds(5).toNanos();
        while(true){var reply=JSON.readTree(gateway.command(command,route,"TEST_ONLY_ORIGINAL_TOKEN",Duration.ofSeconds(2)).toCompletableFuture().join());
            if(!Set.of("OVERLOADED","OUTCOME_UNKNOWN").contains(reply.path("error").path("code").asText())||System.nanoTime()>=end)return reply;
            java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
    }
    void runControl(boolean remote)throws Exception{
        var kit=ActorTestKit.create();
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var callee=f.sender("gateway-command-callee");
            var principal=new AuthPrincipal(new UserId("gateway-command-caller"),new SessionKey("TEST_ONLY",UUID.randomUUID().toString()),Instant.now().plusSeconds(600),Instant.now(),"TEST_ONLY",1);
            var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",key.getPrivate(),Map.of("c001/test",key.getPublic()));var bindings=new ProofBindings(proofs,Clock.systemUTC());
            var verifier=new BoundedTokenVerifier((token,now)->{if(!token.equals("TEST_ONLY_ORIGINAL_TOKEN"))throw new IllegalArgumentException();return principal;},1,8,Duration.ofSeconds(1));
            var registry=new SessionRegistryService(f.runtime.sql,"c001",1,(c,p)->p.equals(principal));
            var policyMode=new java.util.concurrent.atomic.AtomicInteger(1);
            CallAuthorizationPolicy policy=request->policyMode.get()==1?CallAuthorizationPolicy.openAuthenticated("TEST_ONLY_POLICY",Duration.ofSeconds(2)).authorize(request):policyMode.get()==0?CompletableFuture.completedFuture(AuthorizationDecision.denied()):CompletableFuture.failedFuture(new IllegalStateException("TEST_ONLY_DEPENDENCY_OUTAGE"));
            var operations=new NativeSessionOperations(registry,verifier,Clock.systemUTC(),proofs.sessionProofs(),()->true,policy);
            var sessionHandler=new NativeSessionHandler("c001",1,(peer,gateway)->gateway.gatewayId().equals("gw-1"),operations);
            var commands=new CallCommandService(f.runtime.sql,"c001",1,c->{throw new AssertionError();},bindings.commandVerifier("c001",u->new ProofBindings.TrustedHome("c001",1,1))).businessAdmission(()->true);
            var actors=new RpcBusinessHandler.ActorIngress(){
                final Map<CallId,ActorRef<CallMessage>> running=new ConcurrentHashMap<>();
                public void stopFixtureOwner(CallId call){kit.stop(running.get(call));org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->!running.containsKey(call));}
                public CompletionStage<UserCommand.Result> user(UserCommand.Operation op,Instant deadline,int bytes){return CompletableFuture.failedFuture(new AssertionError());}
                public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant deadline,int bytes){return CompletableFuture.failedFuture(new AssertionError());}
                public CompletionStage<CallCommandService.Outcome> call(CallCommand command,String proof,Instant deadline,int bytes){return callTracked(command,proof,deadline,bytes).logical();}
                public synchronized RpcOperation<CallCommandService.Outcome> callTracked(CallCommand command,String proof,Instant deadline,int bytes){
                    var actor=running.computeIfAbsent(command.callId(),call->{var token=f.token(call);var workflow=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",bindings.workflowVerifier("c001",u->new ProofBindings.TrustedHome("c001",1,1)));var backend=new CallCommandHandler(workflow,commands,()->Optional.of(token),1,u->new CallCommandService.TargetHome(remote?"c002":"c001",1));var owner=kit.spawn(CallActor.create(call,backend,()->Optional.of(token),Clock.systemUTC()));kit.spawn(org.apache.pekko.actor.typed.javadsl.Behaviors.<Void>setup(context->{context.watch(owner);return org.apache.pekko.actor.typed.javadsl.Behaviors.receive(Void.class).onSignal(org.apache.pekko.actor.typed.Terminated.class,signal->{running.remove(call,owner);return org.apache.pekko.actor.typed.javadsl.Behaviors.stopped();}).build();}));return owner;});
                    var ask=TrackedEntityAsk.ask(kit.system(),Duration.between(Instant.now(),deadline),CallCommandService.Outcome.class,(reply,receipt)->actor.tell(new CallActor.Execute(command,proof,reply,deadline,bytes,receipt)));
                    return new RpcOperation<>(ask.logical(),ask.physicalCompletion());
                }
            };
            var backend=new RpcBusinessHandler("c001",actors,bindings,proofs,u->new ProofBindings.TrustedHome("c001",1,1),read->{throw new AssertionError();},relay->{throw new AssertionError();},Clock.systemUTC()).businessAdmission(()->true).nativeReads(new NativeSnapshotReads(commands));
            try(verifier;
                var server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",cert("ca.crt"),cert("server.crt"),cert("server.key")),new RpcAdmission(16,1024*1024,16,1024*1024),backend,event->{throw new AssertionError();}).sessions(sessionHandler).start();
                var client=new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key")),new RpcAdmission(16,1024*1024,16,1024*1024));
                var boot=new GatewayBootController(new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c001",1,"TEST_ONLY_REGION"),GatewayBootController.network(client),System::nanoTime)){
                boot.start();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(boot::current);
                var gateway=new NativeGatewayServices(boot,verifier,(p,now)->AuthorizationStatus.ALLOWED,u->new NativeGatewayServices.Home("c001",1),client,new NativeGatewayCommands(boot.identity(),u->new ProofBindings.TrustedHome("c001",1,1),NativeGatewayCommands.network(client),Clock.systemUTC()));
                var route=gateway.register(principal,"TEST_ONLY_ORIGINAL_TOKEN",UUID.randomUUID(),Duration.ofSeconds(2)).toCompletableFuture().join();
                var sender=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),route.connectionGeneration(),route.connectionId());var request=new RequestId(UUID.randomUUID());
                var invite=new CallCommand(SignalEnvelope.Type.INVITE,sender,request,null,CommandScope.invite(),callee.userId(),null,null,"{}","a".repeat(64));
                policyMode.set(0);var denied=JSON.readTree(gateway.command(invite,route,"TEST_ONLY_ORIGINAL_TOKEN",Duration.ofSeconds(2)).toCompletableFuture().join());assertThat(denied.path("error").path("code").asText()).isEqualTo("FORBIDDEN");
                policyMode.set(2);var unavailable=JSON.readTree(gateway.command(invite,route,"TEST_ONLY_ORIGINAL_TOKEN",Duration.ofSeconds(2)).toCompletableFuture().join());assertThat(unavailable.path("error").path("code").asText()).isEqualTo("FORBIDDEN");
                policyMode.set(1);
                var first=retryTransient(gateway,invite,route);
                if(remote){
                    assertThat(first.path("type").asText()).isEqualTo("COMMAND_RESULT");assertThat(first.path("ackCommitted").asBoolean()).isFalse();assertThat(first.path("result").path("status").asText()).isEqualTo("PENDING");
                    var read=new CallCommand(SignalEnvelope.Type.GET_COMMAND_RESULT,sender,request,null,CommandScope.invite(),null,null,null,"{}","b".repeat(64));
                    var pending=retryTransient(gateway,read,route);assertThat(pending.path("result").path("status").asText()).as("native pending lookup: %s",pending).isEqualTo("PENDING");assertThat(pending.path("ackCommitted").asBoolean()).isFalse();
                    try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM command_result WHERE status='FINAL'")){r.next();assertThat(r.getInt(1)).isZero();}return;
                }
                assertThat(first.path("type").asText()).isEqualTo("ACK_COMMITTED");assertThat(first.path("callVersion").isTextual()).isTrue();
                var call=new CallId(first.path("callId").asText());var releasedToken=f.token(call);
                actors.stopFixtureOwner(call); // Force the stale direct fixture ref path deterministically.
                var cancel=new CallCommand(SignalEnvelope.Type.CANCEL,sender,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","c".repeat(64));
                var canceled=retryTransient(gateway,cancel,route);assertThat(canceled.path("type").asText()).as("native cancel: %s",canceled).isEqualTo("ACK_COMMITTED");
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(20)).ignoreExceptionsMatching(error->error instanceof CompletionException&&error.getCause() instanceof AuthoritySql.RetryableConflict).until(()->CoordinatorGrantIT.done(f.groups.releaseTracked(releasedToken)));f.tokens.remove(HomeParticipationService.group(call));
                var callLookup=new CallCommand(SignalEnvelope.Type.GET_COMMAND_RESULT,sender,cancel.requestId(),call,CommandScope.call(call),null,null,null,"{}","d".repeat(64));
                var terminalResult=retryTransient(gateway,callLookup,route);
                assertThat(terminalResult.path("type").asText()).as("native result: %s",terminalResult).isEqualTo("COMMAND_RESULT");assertThat(terminalResult.path("ackCommitted").asBoolean()).isFalse();assertThat(terminalResult.path("result").path("state").asText()).isEqualTo("TERMINAL");
                var absent=new CallCommand(callLookup.type(),sender,new RequestId(UUID.randomUUID()),call,callLookup.scope(),null,null,null,"{}",callLookup.intentHash());
                var missing=retryTransient(gateway,absent,route);assertThat(missing.path("error").path("code").asText()).isEqualTo("RESULT_EXPIRED");
                var lookup=new CallCommand(SignalEnvelope.Type.GET_COMMAND_RESULT,sender,request,null,CommandScope.invite(),null,null,null,"{}","b".repeat(64));
                var recovered=retryTransient(gateway,lookup,route);
                assertThat(recovered.path("type").asText()).isEqualTo("COMMAND_RESULT");assertThat(recovered.path("callId").asText()).isEqualTo(call.value());assertThat(recovered.path("ackCommitted").asBoolean()).isFalse();
                var newer=gateway.register(principal,"TEST_ONLY_ORIGINAL_TOKEN",UUID.randomUUID(),Duration.ofSeconds(2)).toCompletableFuture().join();
                var stale=JSON.readTree(gateway.command(invite,route,"TEST_ONLY_ORIGINAL_TOKEN",Duration.ofSeconds(2)).toCompletableFuture().join());assertThat(stale.path("type").asText()).isEqualTo("ERROR");
                try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM call_state")){r.next();assertThat(r.getInt(1)).isEqualTo(1);}
                assertThat(newer.connectionGeneration()).isEqualTo(2);
                var rebound=new AuthenticatedSession(newer.user(),newer.key(),newer.incarnation(),newer.connectionGeneration(),newer.connectionId());
                var retry=new CallCommand(invite.type(),rebound,request,null,CommandScope.invite(),callee.userId(),null,null,"{}",invite.intentHash());
                var replayed=retryTransient(gateway,retry,newer);assertThat(replayed.path("type").asText()).isEqualTo("ACK_COMMITTED");assertThat(replayed.path("callId").asText()).isEqualTo(call.value());
                try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM call_state")){r.next();assertThat(r.getInt(1)).isEqualTo(1);}
            }
        }finally{kit.shutdownTestKit();}
    }
}
