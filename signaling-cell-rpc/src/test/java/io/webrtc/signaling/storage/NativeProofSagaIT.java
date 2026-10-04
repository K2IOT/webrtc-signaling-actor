package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class NativeProofSagaIT {
    static <T>T done(DbOperation<T> work){try{return work.logical().toCompletableFuture().join();}finally{work.physicalCompletion().toCompletableFuture().join();}}
    @Test void nativeHomeWinnerProofFinalizesOriginalAcceptAndRecordedWinningOperationSurvivesCoordinatorTimeout()throws Exception {run(true);}
    @Test void fullNativeActivationUsesIndependentReplayIdsAndBothConfirmedHomes()throws Exception {run(false);}
    private void run(boolean reconnect)throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("signed-caller");var callee=f.sender("signed-callee");var invite=f.invite(caller,callee.userId());var commands=f.service();var call=commands.executeCallCommand(invite).toCompletableFuture().join().callId();var group=f.token(call);
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));var bindings=new ProofBindings(proofs,Clock.systemUTC());var issuer=new NativeProofIssuer(proofs,Clock.systemUTC(),()->true,g->g.equals(group));var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");var home=new HomeParticipationService(f.runtime.sql,"c001",1,bindings.homeVerifier("c001"));var reads=new HomeProofReadService(home,(c,user)->true);var claim=new AcceptWinnerService(home,(c,r)->true);
            var accept=AcceptCompletionIT.accept(callee,call);assertThat(done(commands.executeUnderAuthorityTracked(accept,new CallCommandService.Authority(call,group,1,"TEST_ONLY_VERIFIED",1),Duration.ofSeconds(2))).status()).isEqualTo("PENDING");
            var winner=new Participant(callee.userId(),callee.key(),callee.incarnation(),1);var transition=new CallWorkflowService.Transition(call,group,1,1,accept.requestId().value(),CallWorkflowService.Step.ACCEPT,winner,List.of(),null,Instant.now().plusSeconds(5),null,"UNSIGNED",null);
            var queryAction=new AuthorizationIntent("QUERY",null,0,null,0,null,null);var calleeQuery=signed(f,issuer,grants,invite,call,callee.userId(),accept.requestId().value(),queryAction);var calleeView=done(reads.observe(calleeQuery,callee,Duration.ofSeconds(2)));var route=calleeView.currentRoutes().getFirst();
            var claimAction=new AuthorizationIntent("CLAIM",calleeView.participation().reservationId(),0,null,0,null,route);var claimRequest=signed(f,issuer,grants,invite,call,callee.userId(),accept.requestId().value(),claimAction);assertThat(done(claim.claimAcceptTracked(claimRequest,calleeView.participation().reservationId(),route,Duration.ofSeconds(2))).outcome()).isEqualTo("CLAIMED");
            var reboundRoute=reconnect?f.sessions.registerSession(SessionAuthReadIT.principal(route),f.boot,UUID.randomUUID(),1).toCompletableFuture().join():route;var resumedCallee=new AuthenticatedSession(reboundRoute.user(),reboundRoute.key(),reboundRoute.incarnation(),reboundRoute.connectionGeneration(),reboundRoute.connectionId());assertThat(resumedCallee.connectionGeneration()).isEqualTo(reconnect?2:1);
            // Coordinator response was lost; native home history keeps the operation to reconcile, not a replacement winner.
            calleeView=done(reads.observe(calleeQuery,resumedCallee,Duration.ofSeconds(2)));assertThat(calleeView.winnerOperation()).isEqualTo(accept.requestId().value());assertThat(calleeView.winnerCallVersion()).isEqualTo(1);
            var callerQuery=signed(f,issuer,grants,invite,call,caller.userId(),accept.requestId().value(),queryAction);var callerView=done(reads.observe(callerQuery,caller,Duration.ofSeconds(2)));var snapshot=done(new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,s)->true).load(call,group,Duration.ofSeconds(2))).orElseThrow();
            var backend=new io.webrtc.signaling.actors.user.PostgresUserBackend(new UserSnapshotService(f.runtime.sql,"c001",1),f.sessions,new UserReservationService(home),claim,new HomeActivationService(home),reads);
            assertThat(done(backend.execute(new io.webrtc.signaling.actors.user.UserCommand.QueryProof(calleeQuery,resumedCallee),Duration.ofSeconds(2))).proofView().winnerOperation()).isEqualTo(accept.requestId().value());
            var nativeWorkflow=new java.util.concurrent.atomic.AtomicReference<CallWorkflowService>();
            var grantReads=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,v)->true);var nativeActors=new RpcBusinessHandler.ActorIngress(){
                public java.util.concurrent.CompletionStage<io.webrtc.signaling.actors.call.CallActor.GrantReply> grant(Request r,AuthorizationIntent action,String destination,Instant deadline,int bytes){return grantReads.load(call,group,Duration.between(Instant.now(),deadline)).logical().thenCompose(current->grants.issue(r,action,group,1,current.orElseThrow().version(),Duration.between(Instant.now(),deadline)).logical()).thenApply(issued->new io.webrtc.signaling.actors.call.CallActor.GrantReply("GRANTED",issued,issuer.coordinator(r,destination,issued)));}
                public java.util.concurrent.CompletionStage<io.webrtc.signaling.actors.user.UserCommand.Result> user(io.webrtc.signaling.actors.user.UserCommand.Operation op,Instant deadline,int bytes){return backend.execute(op,Duration.between(Instant.now(),deadline)).logical();}
                public java.util.concurrent.CompletionStage<CallCommandService.Outcome> call(CallCommand c,String proof,Instant deadline,int bytes){throw new AssertionError();}
                public java.util.concurrent.CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant deadline,int bytes){return nativeWorkflow.get().apply(t,Duration.between(Instant.now(),deadline)).logical();}
            };
            var bridge=new RpcBusinessHandler("c001",nativeActors,bindings,proofs,u->new ProofBindings.TrustedHome("c001",1,1),r->java.util.concurrent.CompletableFuture.failedFuture(new AssertionError()),r->java.util.concurrent.CompletableFuture.failedFuture(new AssertionError()),Clock.systemUTC(),issuer).businessAdmission(()->true);
            var proofClient=new NativeHomeProofClient(nativeActors,(op,wire,budget)->bridge.execute(op,wire,new CellRpcServer.Peer("c001","actor"),budget),Clock.systemUTC());
            var callerSealed=proofClient.prove(callerQuery,"c001",caller,transition,"SESSION",snapshot.caller(),Duration.ofSeconds(2)).toCompletableFuture().join();
            var winnerSealed=proofClient.prove(calleeQuery,"c001",resumedCallee,transition,"WINNER",winner,Duration.ofSeconds(2)).toCompletableFuture().join();
            assertThat(callerSealed.signed()).isNotBlank();assertThat(winnerSealed.signed()).isNotBlank();
            String callerProof=callerSealed.signed();String winnerProof=winnerSealed.signed();Instant expires=proofs.decode(callerProof,"c001",Instant.now()).orElseThrow().expiresAt();Instant other=proofs.decode(winnerProof,"c001",Instant.now()).orElseThrow().expiresAt();if(other.isBefore(expires))expires=other;
            var verified=new CallWorkflowService.Transition(call,group,1,1,accept.requestId().value(),CallWorkflowService.Step.ACCEPT,winner,List.of(),null,expires,null,new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(List.of(callerProof,winnerProof)),null);
            var workflow=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",bindings.workflowVerifier("c001",u->new ProofBindings.TrustedHome("c001",1,1)));nativeWorkflow.set(workflow);var effects=new NativeSagaEffects(nativeActors,(op,wire,budget)->bridge.execute(op,wire,new CellRpcServer.Peer("c001","actor"),budget),phase->new NativeSagaEffects.ProvenCoordinatorStep(transition,new NativeWorkflowExecutor.ProofSpec(callerQuery,"c001",caller,snapshot.caller()),new NativeWorkflowExecutor.ProofSpec(calleeQuery,"c001",resumedCallee,winner)),Clock.systemUTC());assertThat(effects.apply(CrossCellSaga.Phase.ACCEPT_COORDINATOR,accept.requestId().value(),Duration.ofSeconds(2)).toCompletableFuture().join()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");
            assertThat(commands.getCommandResult(resumedCallee,accept.scope(),accept.requestId()).toCompletableFuture().join().orElseThrow().status()).isEqualTo("FINAL");assertThat(done(workflow.apply(verified,Duration.ofSeconds(2))).snapshot().version()).isEqualTo(2);
            if(!reconnect){
                UUID activation=UUID.randomUUID();var callerBefore=proofClient.observe(callerQuery,"c001",caller,Duration.ofSeconds(2)).toCompletableFuture().join();var winnerBefore=proofClient.observe(calleeQuery,"c001",callee,Duration.ofSeconds(2)).toCompletableFuture().join();
                var activationEffects=new NativeSagaEffects(nativeActors,(op,wire,budget)->bridge.execute(op,wire,new CellRpcServer.Peer("c001","actor"),budget),phase->{
                    if(phase==CrossCellSaga.Phase.CONFIRM_CALLER||phase==CrossCellSaga.Phase.CONFIRM_CALLEE){boolean isCaller=phase==CrossCellSaga.Phase.CONFIRM_CALLER;var homeView=isCaller?callerBefore:winnerBefore;var p=homeView.participation();return new NativeSagaEffects.HomeStep("c001",new io.webrtc.signaling.actors.user.UserCommand.Activate(isCaller?callerQuery:calleeQuery,p.reservationId(),p.version(),activation,3,p.winner(),accept.requestId().value()),null);}
                    var step=phase==CrossCellSaga.Phase.ACTIVATE_COORDINATOR?CallWorkflowService.Step.ACTIVATE:CallWorkflowService.Step.READY;
                    var t=new CallWorkflowService.Transition(call,group,1,step==CallWorkflowService.Step.ACTIVATE?2:3,accept.requestId().value(),step,winner,List.of(),activation,Instant.now().plusSeconds(5),null,"PENDING_NATIVE_SIGNATURE",null);
                    return new NativeSagaEffects.ProvenCoordinatorStep(t,new NativeWorkflowExecutor.ProofSpec(callerQuery,"c001",caller,snapshot.caller()),new NativeWorkflowExecutor.ProofSpec(calleeQuery,"c001",callee,winner));
                },Clock.systemUTC());
                var activated=new CrossCellSaga(activationEffects).activate(accept.requestId().value(),Duration.ofSeconds(2)).toCompletableFuture().join();
                assertThat(activated.code()).isEqualTo("CALL_READY");
                var ready=done(workflow.load(call,group,Duration.ofSeconds(2))).orElseThrow();assertThat(ready.state()).isEqualTo("CONNECTING");assertThat(ready.version()).isEqualTo(4);assertThat(ready.activationId()).isEqualTo(activation);
                assertThat(proofClient.observe(callerQuery,"c001",caller,Duration.ofSeconds(2)).toCompletableFuture().join().participation().phase()).isEqualTo("ACTIVE");
                assertThat(proofClient.observe(calleeQuery,"c001",callee,Duration.ofSeconds(2)).toCompletableFuture().join().participation().phase()).isEqualTo("ACTIVE");
            }

        }
    }
    static Request signed(LocalInviteAtomicIT.Fixture f,NativeProofIssuer issuer,CoordinatorGrantService grants,CallCommand invite,CallId call,UserId user,UUID operation,AuthorizationIntent action){Instant now=Instant.now();var group=f.token(call);var unsigned=new Request(user,call,invite.requestId().value(),invite.intentHash(),1,Phase.RINGING,new Grant("c001",1,1,group.group(),group.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));return issuer.coordinator(unsigned,"c001",done(grants.issue(unsigned,action,group,1,1,Duration.ofSeconds(2))));}
}
