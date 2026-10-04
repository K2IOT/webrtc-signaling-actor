package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import java.security.*;
import java.util.concurrent.*;
import io.webrtc.signaling.actors.user.*;

class NativeClaimConsentIT {
    @Test void nativeCoordinatorCannotIssueClaimWithoutCommittedPublicAccept() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var caller=f.sender("consent-caller"); var callee=f.sender("consent-callee");
            var invite=f.invite(caller,callee.userId()); var call=f.service().executeCallCommand(invite).toCompletableFuture().join().callId();
            var group=f.token(call); var accept=AcceptCompletionIT.accept(callee,call);
            var route=SessionAuthReadIT.route(f,callee); Instant now=Instant.now();
            var request=new Request(callee.userId(),call,invite.requestId().value(),invite.intentHash(),1,Phase.RINGING,
                new Grant("c001",1,1,group.group(),group.epoch(),1,accept.requestId().value(),now,now.plusSeconds(5),"UNSIGNED"));
            var action=new AuthorizationIntent("CLAIM",UUID.randomUUID(),0,null,0,null,route);
            var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");
            assertThatThrownBy(()->NativeProofSagaIT.done(grants.issue(request,action,group,1,1,Duration.ofSeconds(2))))
                .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void nativeClaimConsumesOriginalPublicS1ConsentAndRejectsAnotherIntent() throws Exception {
        try (var f=new LocalInviteAtomicIT.Fixture()) {
            var caller=f.sender("s1-caller");var callee=f.sender("s1-callee");var invite=f.invite(caller,callee.userId());
            var commands=f.service();var call=commands.executeCallCommand(invite).toCompletableFuture().join().callId();var group=f.token(call);
            var accept=AcceptCompletionIT.accept(callee,call);NativeProofSagaIT.done(commands.executeUnderAuthorityTracked(accept,new CallCommandService.Authority(call,group,1,"TEST_ONLY_VERIFIED",1),Duration.ofSeconds(2)));
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
            var bindings=new ProofBindings(proofs,Clock.systemUTC());var issuer=new NativeProofIssuer(proofs,Clock.systemUTC(),()->true,group::equals);
            var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");
            var home=new HomeParticipationService(f.runtime.sql,"c001",1,bindings.homeVerifier("c001"));var reads=new HomeProofReadService(home,(c,u)->true);
            var query=NativeProofSagaIT.signed(f,issuer,grants,invite,call,callee.userId(),accept.requestId().value(),new AuthorizationIntent("QUERY",null,0,null,0,null,null));
            var view=NativeProofSagaIT.done(reads.observe(query,callee,Duration.ofSeconds(2)));var route=view.currentRoutes().getFirst();
            var winner=new AcceptWinnerService(home,(c,r)->true);
            var actors=new RpcBusinessHandler.ActorIngress(){
                public CompletionStage<io.webrtc.signaling.actors.call.CallActor.GrantReply> grant(Request r,AuthorizationIntent a,String destination,Instant deadline,int bytes){
                    return grants.issue(r,a,group,1,1,Duration.between(Instant.now(),deadline)).logical().thenApply(i->new io.webrtc.signaling.actors.call.CallActor.GrantReply("GRANTED",i,issuer.coordinator(r,destination,i)));}
                public CompletionStage<UserCommand.Result> user(UserCommand.Operation op,Instant deadline,int bytes){var v=(UserCommand.Accept)op;return winner.claimAcceptTracked(v.request(),v.reservation(),v.route(),Duration.between(Instant.now(),deadline)).logical().thenApply(c->new UserCommand.Result(UserCommand.Code.valueOf(c.outcome()),null,null,c));}
                public CompletionStage<CallCommandService.Outcome> call(CallCommand c,String p,Instant d,int b){throw new AssertionError();}
                public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant d,int b){throw new AssertionError();}
            };
            var bridge=new RpcBusinessHandler("c001",actors,bindings,proofs,u->new ProofBindings.TrustedHome("c001",1,1),r->CompletableFuture.failedFuture(new AssertionError()),r->CompletableFuture.failedFuture(new AssertionError()),Clock.systemUTC(),issuer).businessAdmission(()->true);
            var registry=new SessionRegistryService(f.runtime.sql,"c001",1,(c,p)->true);
            var nativeView=NativeProofSagaIT.done(registry.readCurrentSessionTracked(route,SessionAuthReadIT.principal(route),1,Duration.ofSeconds(2)));
            var operation=new UserCommand.Accept(query,view.participation().reservationId(),route);
            java.util.function.Function<String,CompletionStage<String>> claim=signed->new NativeSagaEffects(actors,(op,wire,budget)->bridge.execute(op,wire,new CellRpcServer.Peer("c001","actor"),budget),phase->new NativeSagaEffects.HomeStep("c001",operation,signed),Clock.systemUTC()).apply(CrossCellSaga.Phase.CLAIM_HOME,accept.requestId().value(),Duration.ofSeconds(2));
            var another=new CallCommand(accept.type(),accept.sender(),accept.requestId(),accept.callId(),accept.scope(),accept.target(),accept.negotiationId(),accept.iceGeneration(),accept.payloadJson(),"f".repeat(64));
            assertThat(claim.apply(proofs.sessionProofs().issue(nativeView,another)).toCompletableFuture().join()).isEqualTo("UNAUTHORIZED");
            assertThat(claim.apply(proofs.sessionProofs().issue(nativeView,accept)).toCompletableFuture().join()).isEqualTo("CLAIMED");
        }
    }

}
