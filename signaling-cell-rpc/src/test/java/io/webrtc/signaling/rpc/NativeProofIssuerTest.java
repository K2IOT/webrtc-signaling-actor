package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class NativeProofIssuerTest {
    final Instant now=Instant.parse("2026-10-03T00:00:00Z");
    final CallId call=new CallId(CrossCellSagaIT.CALL);final UserId user=new UserId("bob");
    final AuthoritySql.GroupToken group=new AuthoritySql.GroupToken("c001",1,1,HomeParticipationService.group(call),2,"TEST_ONLY_OWNER",UUID.randomUUID());
    HomeAuthorizationProof signer(String cell)throws Exception {var k=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();return new HomeAuthorizationProof(cell,"test",k.getPrivate(),Map.of(cell+"/test",k.getPublic()));}
    HomeParticipationService.Request request(){return new HomeParticipationService.Request(user,call,UUID.randomUUID(),"a".repeat(64),1,HomeParticipationService.Phase.PREPARING,new HomeParticipationService.Grant("c001",1,1,group.group(),2,1,UUID.randomUUID(),now,now.plusSeconds(5),"UNSIGNED"));}
    Snapshot snapshot(){return new Snapshot(call,175,1,group.group(),"PREPARING",1,0,new Participant(new UserId("alice"),new SessionKey("TEST_ONLY","caller"),new SessionIncarnation(UUID.randomUUID()),1),user,null,null,"RESERVE_REMOTE","{}","[]","[]",2,null,null,null,new RequestId(UUID.randomUUID()));}
    @Test void coordinatorSignsOnlyNativeProjectionAndClampsToOriginalOwnershipDeadline()throws Exception {
        var signer=signer("c001");var request=request();var issued=new CoordinatorGrantService.Issued(snapshot(),group,7,now,now.plusSeconds(3),HomeParticipationService.authorizationHash(request,HomeParticipationService.AuthorizationIntent.reserve()));
        var issuer=new NativeProofIssuer(signer,Clock.fixed(now,ZoneOffset.UTC),()->true,g->g.equals(group));
        var signed=issuer.coordinator(request,"c002",issued);var claims=signer.decode(signed.grant().proof(),"c001",now).orElseThrow();
        assertThat(claims.expiresAt()).isEqualTo(now.plusSeconds(3));assertThat(claims.callVersion()).isEqualTo(1);assertThat(claims.ownerIncarnation()).isEqualTo(group.incarnation());assertThat(signed.grant().sequence()).isEqualTo(7);
        assertThat(new ProofBindings(signer,Clock.fixed(now,ZoneOffset.UTC)).homeVerifier("c002").verify(signed)).isTrue();
        assertThatThrownBy(()->new NativeProofIssuer(signer,Clock.fixed(now.plusSeconds(3),ZoneOffset.UTC),()->true,g->true).coordinator(request,"c002",issued)).isInstanceOf(AuthoritySql.FencedException.class);
        assertThatThrownBy(()->new NativeProofIssuer(signer,Clock.fixed(now,ZoneOffset.UTC),()->false,g->true).coordinator(request,"c002",issued)).isInstanceOf(AuthoritySql.FencedException.class);
        assertThatThrownBy(()->new NativeProofIssuer(signer,Clock.fixed(now,ZoneOffset.UTC),()->true,g->false).coordinator(request,"c002",issued)).isInstanceOf(AuthoritySql.FencedException.class);
    }
    @Test void homeCannotSignFabricatedWinnerOrRouteAndNeverResetsReadExpiry()throws Exception {
        var signer=signer("c002");var issuer=new NativeProofIssuer(signer,Clock.fixed(now,ZoneOffset.UTC),()->true,g->false);var request=request();
        var participant=new Participant(user,new SessionKey("TEST_ONLY","callee"),new SessionIncarnation(UUID.randomUUID()),1);
        var route=new SessionRepository.Route(user,participant.key(),participant.incarnation(),1,"gw",UUID.randomUUID(),UUID.randomUUID(),now.plusSeconds(60),"test",1);
        var participation=new HomeParticipationService.Participation(call,user,request.acquireOperation(),request.payloadHash(),"RINGING",UUID.randomUUID(),1,now.plusSeconds(30),null,2);
        var view=new HomeProofReadService.View(participation,List.of(route),null,0,now,"c002",1,1);
        var transition=new CallWorkflowService.Transition(call,group,1,2,UUID.randomUUID(),CallWorkflowService.Step.ACCEPT,participant,List.of(),null,now.plusSeconds(5),null,"UNSIGNED",null);
        assertThatThrownBy(()->issuer.home(request,view,transition,"WINNER",participant)).isInstanceOf(AuthoritySql.FencedException.class);
        var stale=new HomeProofReadService.View(participation,List.of(route),null,0,now.minusSeconds(5),"c002",1,1);
        assertThatThrownBy(()->issuer.home(request,stale,transition,"SESSION",participant)).isInstanceOf(AuthoritySql.FencedException.class);
    }
}
