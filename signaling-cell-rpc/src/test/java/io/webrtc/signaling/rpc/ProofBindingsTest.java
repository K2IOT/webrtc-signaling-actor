package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class ProofBindingsTest {
    @Test void nativeHomeGrantVerifierRejectsChangedPulseAcquisitionIntentAndDestination()throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var signer=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));var bindings=new ProofBindings(signer,Clock.systemUTC());Instant now=Instant.now();UUID op=UUID.randomUUID();var call=new CallId(CrossCellSagaIT.CALL);
        var unsigned=new HomeParticipationService.Request(new UserId("bob"),call,UUID.randomUUID(),"a".repeat(64),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,685,2,1,op,now,now.plusSeconds(5),"UNSIGNED"));
        var claims=new HomeAuthorizationProof.Claims(1,"COORDINATOR_GRANT","c001","c002",op,call,unsigned.user(),null,null,0,1,1,1,685,2,1,UUID.randomUUID(),null,0,null,0,0,ProofBindings.homeIntent(unsigned),now,now.plusSeconds(5),1,null);String proof=signer.issue(claims);
        var request=replace(unsigned,unsigned.acquireOperation(),1,proof);assertThat(bindings.homeVerifier("c002").verify(request)).isTrue();assertThat(bindings.homeVerifier("c003").verify(request)).isFalse();assertThat(bindings.homeVerifier("c002").verify(replace(unsigned,unsigned.acquireOperation(),2,proof))).isFalse();assertThat(bindings.homeVerifier("c002").verify(replace(unsigned,UUID.randomUUID(),1,proof))).isFalse();
        assertThat(bindings.homeVerifier("c002").verify(request,new HomeParticipationService.AuthorizationIntent("RELEASE",null,0,null,0,null,null))).isFalse();
    }
    private static HomeParticipationService.Request replace(HomeParticipationService.Request r,UUID acquire,long sequence,String proof){var g=r.grant();return new HomeParticipationService.Request(r.user(),r.call(),acquire,r.payloadHash(),r.directoryEpoch(),r.phase(),new HomeParticipationService.Grant(g.cell(),g.storageEpoch(),g.hashVersion(),g.group(),g.groupEpoch(),sequence,g.operation(),g.issuedAt(),g.expiresAt(),proof));}
    @Test void coordinatorCommandProofRequiresCurrentHomeSessionAndStorageEpochForExactRequest()throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var signer=new HomeAuthorizationProof("c002","test",keys.getPrivate(),Map.of("c002/test",keys.getPublic()));Instant now=Instant.now();var call=new CallId(CrossCellSagaIT.CALL);var sender=new AuthenticatedSession(new UserId("bob"),new SessionKey("TEST_ONLY","jti"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());
        var command=new CallCommand(SignalEnvelope.Type.ACCEPT,sender,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","a".repeat(64));
        var proof=new HomeAuthorizationProof.Claims(1,"SESSION","c002","c001",command.requestId().value(),call,sender.userId(),sender.key(),sender.incarnation(),1,1,1,1,685,2,1,UUID.randomUUID(),UUID.randomUUID(),1,null,2,0,command.intentHash(),now,now.plusSeconds(5),1,now.plusSeconds(30));
        var bindings=new ProofBindings(signer,Clock.systemUTC());String signed=signer.issue(proof);var accepted=bindings.commandVerifier("c001",user->new ProofBindings.TrustedHome("c002",1,1));assertThat(accepted.verify(command,null,signed)).isTrue();assertThat(bindings.commandVerifier("c001",user->new ProofBindings.TrustedHome("c002",1,2)).verify(command,null,signed)).isFalse();
        var rebound=new AuthenticatedSession(sender.userId(),sender.key(),sender.incarnation(),2,UUID.randomUUID());var changed=new CallCommand(command.type(),rebound,command.requestId(),call,command.scope(),null,null,null,"{}",command.intentHash());assertThat(accepted.verify(changed,null,signed)).isFalse();
    }
}
