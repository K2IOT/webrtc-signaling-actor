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
    @Test void nativeHomeWinnerProofFinalizesOriginalAcceptAndRecordedWinningOperationSurvivesCoordinatorTimeout()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("signed-caller");var callee=f.sender("signed-callee");var invite=f.invite(caller,callee.userId());var commands=f.service();var call=commands.executeCallCommand(invite).toCompletableFuture().join().callId();var group=f.token(call);
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));var bindings=new ProofBindings(proofs,Clock.systemUTC());var issuer=new NativeProofIssuer(proofs,Clock.systemUTC(),()->true,g->g.equals(group));var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");var home=new HomeParticipationService(f.runtime.sql,"c001",1,bindings.homeVerifier("c001"));var reads=new HomeProofReadService(home,(c,user)->true);var claim=new AcceptWinnerService(home);
            var accept=AcceptCompletionIT.accept(callee,call);assertThat(done(commands.executeUnderAuthorityTracked(accept,new CallCommandService.Authority(call,group,1,"TEST_ONLY_VERIFIED",1),Duration.ofSeconds(2))).status()).isEqualTo("PENDING");
            var winner=new Participant(callee.userId(),callee.key(),callee.incarnation(),1);var transition=new CallWorkflowService.Transition(call,group,1,1,accept.requestId().value(),CallWorkflowService.Step.ACCEPT,winner,List.of(),null,Instant.now().plusSeconds(5),null,"UNSIGNED",null);
            var queryAction=new AuthorizationIntent("QUERY",null,0,null,0,null,null);var calleeQuery=signed(f,issuer,grants,invite,call,callee.userId(),accept.requestId().value(),queryAction);var calleeView=done(reads.observe(calleeQuery,callee,Duration.ofSeconds(2)));var route=calleeView.currentRoutes().getFirst();
            var claimAction=new AuthorizationIntent("CLAIM",calleeView.participation().reservationId(),0,null,0,null,route);var claimRequest=signed(f,issuer,grants,invite,call,callee.userId(),accept.requestId().value(),claimAction);assertThat(done(claim.claimAcceptTracked(claimRequest,calleeView.participation().reservationId(),route,Duration.ofSeconds(2))).outcome()).isEqualTo("CLAIMED");
            // Coordinator response was lost; native home history keeps the operation to reconcile, not a replacement winner.
            calleeView=done(reads.observe(calleeQuery,callee,Duration.ofSeconds(2)));assertThat(calleeView.winnerOperation()).isEqualTo(accept.requestId().value());assertThat(calleeView.winnerCallVersion()).isEqualTo(1);
            var callerQuery=signed(f,issuer,grants,invite,call,caller.userId(),accept.requestId().value(),queryAction);var callerView=done(reads.observe(callerQuery,caller,Duration.ofSeconds(2)));var snapshot=done(new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,s)->true).load(call,group,Duration.ofSeconds(2))).orElseThrow();
            var backend=new io.webrtc.signaling.actors.user.PostgresUserBackend(new UserSnapshotService(f.runtime.sql,"c001",1),f.sessions,new UserReservationService(home),claim,new HomeActivationService(home),reads);
            assertThat(done(backend.execute(new io.webrtc.signaling.actors.user.UserCommand.QueryProof(calleeQuery,callee),Duration.ofSeconds(2))).proofView().winnerOperation()).isEqualTo(accept.requestId().value());
            String callerProof=issuer.home(callerQuery,callerView,transition,"SESSION",snapshot.caller());String winnerProof=issuer.home(calleeQuery,calleeView,transition,"WINNER",winner);Instant expires=proofs.decode(callerProof,"c001",Instant.now()).orElseThrow().expiresAt();Instant other=proofs.decode(winnerProof,"c001",Instant.now()).orElseThrow().expiresAt();if(other.isBefore(expires))expires=other;
            var verified=new CallWorkflowService.Transition(call,group,1,1,accept.requestId().value(),CallWorkflowService.Step.ACCEPT,winner,List.of(),null,expires,null,new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(List.of(callerProof,winnerProof)),null);
            var workflow=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",bindings.workflowVerifier("c001",u->new ProofBindings.TrustedHome("c001",1,1)));assertThat(done(workflow.apply(verified,Duration.ofSeconds(2))).code()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");
            assertThat(commands.getCommandResult(callee,accept.scope(),accept.requestId()).toCompletableFuture().join().orElseThrow().status()).isEqualTo("FINAL");assertThat(done(workflow.apply(verified,Duration.ofSeconds(2))).snapshot().version()).isEqualTo(2);
        }
    }
    static Request signed(LocalInviteAtomicIT.Fixture f,NativeProofIssuer issuer,CoordinatorGrantService grants,CallCommand invite,CallId call,UserId user,UUID operation,AuthorizationIntent action){Instant now=Instant.now();var group=f.token(call);var unsigned=new Request(user,call,invite.requestId().value(),invite.intentHash(),1,Phase.RINGING,new Grant("c001",1,1,group.group(),group.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));return issuer.coordinator(unsigned,"c001",done(grants.issue(unsigned,action,group,1,1,Duration.ofSeconds(2))));}
}
