package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class CriticalCommandProofTest {
    @Test void newRoundRequiresBothNativeActiveBindingsSameActivationRootAndVersion()throws Exception{
        var a=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var b=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var trusted=Map.of("c001/test",a.getPublic(),"c002/test",b.getPublic());var signerA=new HomeAuthorizationProof("c001","test",a.getPrivate(),trusted);var signerB=new HomeAuthorizationProof("c002","test",b.getPrivate(),trusted);var call=new CallId("c001.e1.00000000-0000-0000-0000-000000000001");var caller=session("caller");var winner=session("winner");UUID activation=UUID.randomUUID(),root=UUID.randomUUID();var token=new AuthoritySql.GroupToken("c001",1,1,HomeParticipationService.group(call),2,"TEST_ONLY_OWNER",root);var command=new CallCommand(SignalEnvelope.Type.NEGOTIATE_REQUEST,caller,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","c".repeat(64));
        var snapshot=new Snapshot(call,1,1,token.group(),"CONNECTING",4,0,participant(caller),winner.userId(),participant(winner),activation,"ACTIVE","{}","[]","[]",2,null,null,null,new RequestId(UUID.randomUUID()));Instant now=Instant.now();var route=new SessionRepository.Route(caller.userId(),caller.key(),caller.incarnation(),1,"TEST_ONLY",UUID.randomUUID(),caller.connectionId(),now.plusSeconds(30),"TEST_ONLY",1);String session=signerA.sessionProofs().issue(new SessionRegistryService.SessionProofView(route,now,now.plusSeconds(4),"c001",1,1),command);String activeA=active(signerA,caller,command,snapshot,token,now),activeB=active(signerB,winner,command,snapshot,token,now);
        var bundle=new CriticalCommandProof(session,activeA,activeB);String proof=bundle.encode();var bindings=new ProofBindings(signerA,Clock.systemUTC());java.util.function.Function<UserId,ProofBindings.TrustedHome> homes=user->new ProofBindings.TrustedHome(user.equals(caller.userId())?"c001":"c002",1,1);var authority=new CallCommandService.Authority(call,token,1,proof,4);
        assertThat(bindings.commandVerifier("c001",homes).verify(command,snapshot,proof)).isTrue();var evidence=bindings.negotiationVerifier("c001",homes).verify(null,command,snapshot,authority);assertThat(evidence).isNotNull();assertThat(evidence.recipient()).isEqualTo(winner);assertThat(evidence.participantUntil()).isEqualTo(now.plusSeconds(30));
        assertThat(bindings.negotiationVerifier("c001",homes).verify(null,command,snapshot,new CallCommandService.Authority(call,new AuthoritySql.GroupToken("c001",1,1,token.group(),3,"TEST_ONLY_NEXT",UUID.randomUUID()),1,proof,4))).isNull();
        assertThat(bindings.negotiationVerifier("c001",homes).verify(null,command,snapshot,new CallCommandService.Authority(call,token,1,new CriticalCommandProof(session,activeA,activeA).encode(),4))).isNull();
        assertThat(bindings.negotiationVerifier("c001",homes).verify(null,command,snapshot,new CallCommandService.Authority(call,token,1,session,4))).isNull();
    }
    static AuthenticatedSession session(String user){return new AuthenticatedSession(new UserId(user),new SessionKey("TEST_ONLY",user),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());}
    static Participant participant(AuthenticatedSession sender){return new Participant(sender.userId(),sender.key(),sender.incarnation(),sender.connectionGeneration());}
    static String active(HomeAuthorizationProof signer,AuthenticatedSession sender,CallCommand command,Snapshot snapshot,AuthoritySql.GroupToken token,Instant now){return signer.issue(new HomeAuthorizationProof.Claims(1,"ACTIVE",signer.sourceCell(),"c001",command.requestId().value(),command.callId(),sender.userId(),sender.key(),sender.incarnation(),1,1,1,1,token.group(),token.epoch(),1,token.incarnation(),UUID.randomUUID(),2,snapshot.activationId(),snapshot.version(),snapshot.negotiationId(),command.intentHash(),now,now.plusSeconds(4),1,now.plusSeconds(30),sender.connectionId()));}
}
