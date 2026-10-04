package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import java.time.*;
import java.security.*;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Local native epoch-transition contracts. These tests do not certify physical PostgreSQL HA fencing. */
class DatabaseFailoverIT {
    static void testOnlyInstallEpoch(LocalInviteAtomicIT.Fixture f,long next)throws Exception{
        try(var c=f.connection()){
            c.setAutoCommit(false);AuthoritySql.cellBarrier(c,true);AuthoritySql.validateCell(c,"c001",next-1);
            try(var q=c.prepareStatement("UPDATE cell_authority SET storage_epoch=? WHERE singleton_id=1 AND storage_epoch=?")){q.setLong(1,next);q.setLong(2,next-1);assertThat(q.executeUpdate()).isEqualTo(1);}c.commit();
        }
    }
    @Test void nativeStorageEpochTransitionRetainsCallRoutingIdentityAndFencesEveryOldRoot()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("promotion-caller");var callee=f.sender("promotion-callee");var invite=f.invite(caller,callee.userId());var result=f.service().executeCallCommand(invite).toCompletableFuture().join();var call=result.callId();var old=f.token(call);
            var oldWorkflow=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,s)->false);
            var original=CoordinatorGrantIT.done(oldWorkflow.load(call,old,Duration.ofSeconds(2))).orElseThrow();
            testOnlyInstallEpoch(f,2);
            var owner="TEST_ONLY_PROMOTED_OWNER";var roots=new GroupOwnerRepository(f.runtime.sql,"c001",2);var acquired=CoordinatorGrantIT.done(roots.acquireTracked(old.group(),owner,UUID.randomUUID(),UUID.randomUUID())).orElseThrow();
            assertThat(acquired.token().storageEpoch()).isEqualTo(2);assertThat(acquired.token().epoch()).isGreaterThan(old.epoch());
            assertThatThrownBy(()->CoordinatorGrantIT.done(f.groups.pulseTracked(new GroupOwnerRepository.Grant(old,1,UUID.randomUUID(),UUID.randomUUID(),Instant.now().plusSeconds(10),Instant.now()),2,UUID.randomUUID()))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->CoordinatorGrantIT.done(oldWorkflow.load(call,old,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            var workflow=new CallWorkflowService(f.runtime.sql,"c001",2,owner,(t,s)->false);
            var recovered=CoordinatorGrantIT.done(workflow.loadCold(call,acquired.token(),Duration.ofSeconds(2))).orElseThrow();
            assertThat(recovered.callId()).isEqualTo(call);assertThat(recovered.callId().routingEpoch()).isEqualTo(1);assertThat(recovered.state()).isEqualTo("RINGING");assertThat(recovered.version()).isEqualTo(original.version());assertThat(recovered.deadlines()).isEqualTo(original.deadlines());
            var now=Instant.now();var request=new HomeParticipationService.Request(caller.userId(),call,invite.requestId().value(),invite.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",2,1,old.group(),acquired.token().epoch(),acquired.sequence(),UUID.randomUUID(),now,now.plusSeconds(5),"UNSIGNED"));
            var action=new HomeParticipationService.AuthorizationIntent("QUERY",null,0,null,0,null,null);var issued=CoordinatorGrantIT.done(new CoordinatorGrantService(f.runtime.sql,"c001",2,owner).issue(request,action,acquired.token(),1,1,Duration.ofSeconds(2)));
            var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",key.getPrivate(),Map.of("c001/test",key.getPublic()));
            var signed=new NativeProofIssuer(proofs,Clock.systemUTC(),()->true,token->token.equals(acquired.token())).coordinator(request,"c001",issued);
            assertThat(new ProofBindings(proofs,Clock.systemUTC()).homeVerifier("c001").verify(signed,action)).isTrue();
            assertThat(signed.call()).isEqualTo(call);assertThat(signed.grant().storageEpoch()).isEqualTo(2);
        }
    }
    @Test void newCallsUseTheConfiguredRoutingEpochRatherThanThePromotedStorageEpoch()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("new-epoch-caller");var callee=f.sender("new-epoch-callee");var oldCaller=SessionAuthReadIT.route(f,caller);var oldCallee=SessionAuthReadIT.route(f,callee);
            testOnlyInstallEpoch(f,2);var sessions=new SessionRegistryService(f.runtime.sql,"c001",2,(c,p)->true);var boot=sessions.startGatewayBoot("TEST_ONLY_AFTER_PROMOTION",UUID.randomUUID(),"TEST_ONLY_REGION",UUID.randomUUID()).toCompletableFuture().join();
            var callerRoute=sessions.registerSession(SessionAuthReadIT.principal(oldCaller),boot,UUID.randomUUID(),1).toCompletableFuture().join();sessions.registerSession(SessionAuthReadIT.principal(oldCallee),boot,UUID.randomUUID(),1).toCompletableFuture().join();
            var sender=new AuthenticatedSession(callerRoute.user(),callerRoute.key(),callerRoute.incarnation(),callerRoute.connectionGeneration(),callerRoute.connectionId());
            var call=CallId.create("c001",1);var roots=new GroupOwnerRepository(f.runtime.sql,"c001",2);var token=CoordinatorGrantIT.done(roots.acquireTracked(HomeParticipationService.group(call),"TEST_ONLY_NEW_ROUTING",UUID.randomUUID(),UUID.randomUUID())).orElseThrow().token();
            var commands=new CallCommandService(f.runtime.sql,"c001",2,1,c->{throw new AssertionError();},(command,snapshot,proof)->proof.equals("TEST_ONLY"),null).businessAdmission(()->true);
            var invite=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.INVITE,sender,new RequestId(UUID.randomUUID()),call,CommandScope.invite(),callee.userId(),null,null,"{}","a".repeat(64));
            var outcome=CoordinatorGrantIT.done(commands.executeUnderAuthorityTracked(invite,new CallCommandService.Authority(call,token,1,"TEST_ONLY",0,new CallCommandService.TargetHome("c001",1)),Duration.ofSeconds(2)));
            assertThat(outcome.callId().routingEpoch()).isEqualTo(1);assertThat(outcome.status()).isEqualTo("FINAL");assertThat(outcome.state()).isEqualTo("RINGING");
            var wrong=CallId.create("c001",2);var wrongToken=HomeParticipationService.group(wrong)==token.group()?token:CoordinatorGrantIT.done(roots.acquireTracked(HomeParticipationService.group(wrong),"TEST_ONLY_WRONG_ROUTING",UUID.randomUUID(),UUID.randomUUID())).orElseThrow().token();
            var unapproved=new io.webrtc.signaling.protocol.CallCommand(invite.type(),sender,new RequestId(UUID.randomUUID()),wrong,invite.scope(),callee.userId(),null,null,"{}",invite.intentHash());
            assertThatThrownBy(()->CoordinatorGrantIT.done(commands.executeUnderAuthorityTracked(unapproved,new CallCommandService.Authority(wrong,wrongToken,1,"TEST_ONLY",0,new CallCommandService.TargetHome("c001",1)),Duration.ofSeconds(2)))).hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
        }
    }

    static HomeParticipationService.Request promoted(HomeParticipationService.Request old,long storage,long group,long sequence){
        Instant now=Instant.now();return new HomeParticipationService.Request(old.user(),old.call(),old.acquireOperation(),old.payloadHash(),old.directoryEpoch(),old.phase(),new HomeParticipationService.Grant(old.grant().cell(),storage,1,old.grant().group(),group,sequence,UUID.randomUUID(),now,now.plusSeconds(5),"TEST_ONLY_VERIFIED"));
    }
    static HomeParticipationService.Request remoteCoordinatorRequest(String user){
        var call=CallId.create("c002",1);var old=UserReservationRaceIT.request(user,call);var g=old.grant();
        return new HomeParticipationService.Request(old.user(),call,old.acquireOperation(),old.payloadHash(),old.directoryEpoch(),old.phase(),new HomeParticipationService.Grant("c002",1,1,g.group(),g.groupEpoch(),g.sequence(),g.operation(),g.issuedAt(),g.expiresAt(),g.proof()));
    }
    @Test void verifiedPromotionAdoptsOnlyUnexpiredReservationsAndRetainsWinnerAndReplayExpiry()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var sender=f.sender("adoption-user");var old=remoteCoordinatorRequest(sender.userId().value());
            var home=new HomeParticipationService(f.runtime.sql,"c001",1,r->r.grant().proof().equals("TEST_ONLY_VERIFIED"),(cell,from,to)->cell.equals("c002")&&from==1&&to==2);
            var reservations=new UserReservationService(home);var first=reservations.reserveUser(old).toCompletableFuture().join();
            var claimed=new AcceptWinnerService(home,(c,r)->true).claimAccept(old,first.reservationId(),SessionAuthReadIT.route(f,sender)).toCompletableFuture().join();
            var next=promoted(old,2,2,1);
            var closed=new UserReservationService(new HomeParticipationService(f.runtime.sql,"c001",1,r->true));
            assertThatThrownBy(()->closed.renewReservation(next,first.reservationId(),claimed.version()).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            var adopted=reservations.renewReservation(next,first.reservationId(),claimed.version()).toCompletableFuture().join();
            assertThat(adopted.reservationId()).isEqualTo(first.reservationId());assertThat(adopted.winner()).isEqualTo(claimed.winner());assertThat(adopted.call()).isEqualTo(old.call());
            assertThat(reservations.renewReservation(next,first.reservationId(),claimed.version()).toCompletableFuture().join()).isEqualTo(adopted);
            assertThatThrownBy(()->reservations.renewReservation(promoted(old,1,3,2),first.reservationId(),adopted.version()).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT h.coordinator_storage_epoch,r.coordinator_storage_epoch FROM home_participation h JOIN user_reservation r USING(call_id,user_id)")){r.next();assertThat(r.getLong(1)).isEqualTo(2);assertThat(r.getLong(2)).isEqualTo(2);}
            reservations.releaseIfCallVersion(next,adopted.reservationId(),adopted.version()).toCompletableFuture().join();
            assertThat(reservations.reserveUser(next).toCompletableFuture().join().terminal()).isTrue();
            assertThatThrownBy(()->reservations.reserveUser(promoted(old,3,3,1)).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            var expiring=remoteCoordinatorRequest("expired-adoption");var reserved=reservations.reserveUser(expiring).toCompletableFuture().join();
            try(var c=f.connection();var q=c.prepareStatement("UPDATE user_reservation SET lease_until=clock_timestamp()-interval '1 second' WHERE user_id=?")){q.setString(1,expiring.user().value());q.executeUpdate();}
            assertThatThrownBy(()->reservations.renewReservation(promoted(expiring,2,2,1),reserved.reservationId(),reserved.version()).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }

    @Test void gatewayAllocatesConfiguredRoutingEpochEvenWhenItsNativeHomeStorageHasAdvanced()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var oldSender=f.sender("gateway-routing");var route=SessionAuthReadIT.route(f,oldSender);var sender=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),route.connectionGeneration(),route.connectionId());var candidate=new java.util.concurrent.atomic.AtomicReference<CallId>();
            var network=new io.webrtc.signaling.gateway.NativeGatewayCommands.Network(){
                public java.util.concurrent.CompletionStage<io.webrtc.signaling.protocol.internal.SessionReply> session(io.webrtc.signaling.protocol.internal.SessionCommand wire,Duration budget){
                    try{var request=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules().readValue(wire.getPayload().toByteArray(),NativeSessionHandler.Request.class);candidate.set(request.proofCommand().callId());}catch(Exception invalid){throw new AssertionError(invalid);}
                    return java.util.concurrent.CompletableFuture.completedFuture(io.webrtc.signaling.protocol.internal.SessionReply.newBuilder().setOperationId(wire.getOperationId()).setStatus("REJECTED").setErrorCode("FORBIDDEN").build());
                }
                public java.util.concurrent.CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply> call(CellRpcServer.Operation op,io.webrtc.signaling.protocol.internal.InternalCommand c,Duration b){throw new AssertionError();}
            };
            var gateway=new NativeSessionHandler.GatewayIdentity(route.gatewayId(),route.bootId(),"c001",2,"TEST_ONLY");
            var commands=new io.webrtc.signaling.gateway.NativeGatewayCommands(gateway,u->new ProofBindings.TrustedHome("c001",1,2),network,Clock.systemUTC(),1);
            commands.execute(f.invite(sender,new UserId("callee")),route,"TEST_ONLY",Duration.ofSeconds(2)).toCompletableFuture().join();assertThat(candidate.get()).as("captured proof candidate").isNotNull();assertThat(candidate.get().routingEpoch()).isEqualTo(1);
        }
    }

    @Test void admittedPromotionCommitsNewEpochWithoutAbortingCallsAndRejectsUntrustedReplay()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("ha-api-caller");var callee=f.sender("ha-api-callee");var command=f.invite(caller,callee.userId());var result=f.service().executeCallCommand(command).toCompletableFuture().join();
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var now=Instant.now();
            var unsigned=new StoragePromotionService.Permit("c001",1,2,UUID.randomUUID(),now,now.plusSeconds(5),"a".repeat(64),"b".repeat(64),"TEST_ONLY_HA_SOURCE","");
            var signer=Signature.getInstance("Ed25519");signer.initSign(keys.getPrivate());signer.update(StoragePromotionService.signingBytes(unsigned));var signature=Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign());
            var permit=new StoragePromotionService.Permit(unsigned.cell(),1,2,unsigned.operation(),now,now.plusSeconds(5),unsigned.oldPrimaryFenceReceiptSha256(),unsigned.acknowledgedWalReceiptSha256(),unsigned.keyId(),signature);
            var admission=StoragePromotionService.enrolledVerifier(List.of(new StoragePromotionService.EnrolledSource("TEST_ONLY_HA_SOURCE","c001",keys.getPublic())));
            var promotion=new StoragePromotionService(f.runtime.sql,"c001",admission);
            assertThat(CoordinatorGrantIT.done(promotion.promote(permit))).isEqualTo(2);assertThat(CoordinatorGrantIT.done(promotion.promote(permit))).isEqualTo(2);
            try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT s.state,s.version,(SELECT count(*) FROM user_reservation),(SELECT count(*) FROM promotion_epoch_journal) FROM call_state s")){r.next();assertThat(r.getString(1)).isEqualTo("RINGING");assertThat(r.getLong(2)).isEqualTo(result.version());assertThat(r.getInt(3)).isEqualTo(2);assertThat(r.getInt(4)).isEqualTo(1);}
            var tampered=new StoragePromotionService.Permit(permit.cell(),1,3,permit.operation(),now,now.plusSeconds(5),permit.oldPrimaryFenceReceiptSha256(),permit.acknowledgedWalReceiptSha256(),permit.keyId(),permit.signedEvidence());
            assertThatThrownBy(()->promotion.promote(tampered)).isInstanceOf(AuthoritySql.FencedException.class);
            var deny=new StoragePromotionService(f.runtime.sql,"c001",StoragePromotionService.enrolledVerifier(List.of()));assertThatThrownBy(()->deny.promote(permit)).isInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->CoordinatorGrantIT.done(f.groups.pulseTracked(new GroupOwnerRepository.Grant(new AuthoritySql.GroupToken("c001",1,1,HomeParticipationService.group(result.callId()),2,"TEST_ONLY_LOCAL_OWNER",f.incarnation),1,UUID.randomUUID(),UUID.randomUUID(),now.plusSeconds(10),now),2,UUID.randomUUID()))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }

}
