package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import java.io.File;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class CrossCellSagaIT {
    static final String CALL="c001.e1.00000000-0000-0000-0000-000000000001";
    static File cert(String name){return new File(Objects.requireNonNull(CrossCellSagaIT.class.getResource("/test-only-pki/"+name)).getFile());}
    static io.netty.handler.ssl.SslContext serverTls()throws Exception{return GrpcSslContexts.forServer(cert("server.crt"),cert("server.key")).trustManager(cert("ca.crt")).clientAuth(ClientAuth.REQUIRE).protocols("TLSv1.3").build();}
    static io.netty.handler.ssl.SslContext clientTls(String role)throws Exception{return GrpcSslContexts.forClient().trustManager(cert("ca.crt")).keyManager(cert(role+".crt"),cert(role+".key")).protocols("TLSv1.3").build();}
    static InternalCommand command(String destination){return InternalCommand.newBuilder().setSchemaMajor(1).setSchemaMinor(0).setOperationId(UUID.randomUUID().toString()).setType("ReserveUser").setDestinationCell(destination).setCallId(CALL).setCommandScope("CALL:"+CALL).setPayloadHash(ByteString.copyFrom(new byte[32])).setRemainingBudgetMs(2000).setPayload(ByteString.copyFromUtf8("{}" )).build();}
    static InternalReply committed(InternalCommand c){return InternalReply.newBuilder().setOperationId(c.getOperationId()).setAckCommitted(true).setStatus("COMMITTED").setCallId(c.getCallId()).setCallVersion(1).build();}
    static CellRpcClient client(int port,String role)throws Exception{return new CellRpcClient(Map.of("c002",new CellRpcClient.Endpoint("localhost",port,"localhost")),clientTls(role),new RpcAdmission(64,1024*1024,64,1024*1024));}
    @Test void controlAndRelayCreditsAndRetainedBytesArePhysicallyIsolated(){
        var admission=new RpcAdmission(1,1024,2,2048);var control=admission.acquire(RpcAdmission.Lane.CONTROL,1024);assertThatThrownBy(()->admission.acquire(RpcAdmission.Lane.CONTROL,1)).isInstanceOf(RpcAdmission.Overloaded.class);
        try(var relay=admission.acquire(RpcAdmission.Lane.RELAY,1024)){assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);assertThatThrownBy(()->admission.acquire(RpcAdmission.Lane.RELAY,1025)).isInstanceOf(RpcAdmission.Overloaded.class);}
        control.close();assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();
    }
    @Test void signedHomeProofIsRequestAndAuthorityBoundDuplicateSafeAndExpiresWithoutRenewal()throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proof=new HomeAuthorizationProof("c001","test-key",keys.getPrivate(),Map.of("c001/test-key",keys.getPublic()));Instant now=Instant.now();
        var expected=new HomeAuthorizationProof.Claims(1,"COORDINATOR_GRANT","c001","c002",UUID.randomUUID(),new CallId(CALL),new UserId("bob"),new SessionKey("TEST_ONLY","jti"),new SessionIncarnation(UUID.randomUUID()),1,1,1,1,685,2,1,UUID.randomUUID(),UUID.randomUUID(),1,null,1,0,"a".repeat(64),now,now.plusSeconds(5),1,now.plusSeconds(30));
        String signed=proof.issue(expected);assertThat(proof.verify(signed,expected,"c001",now.plusMillis(1))).isTrue();assertThat(proof.verify(signed,expected,"c001",now.plusMillis(2))).isTrue();assertThat(proof.verify(signed,expected,"c002",now)).isFalse();assertThat(proof.verify(signed,expected,"c001",now.plusSeconds(5))).isFalse();assertThat(proof.verify(signed.substring(0,signed.length()-4)+"AAAA",expected,"c001",now)).isFalse();
        var other=new HomeAuthorizationProof.Claims(1,expected.purpose(),expected.sourceCell(),"c003",expected.operation(),expected.call(),expected.user(),expected.session(),expected.incarnation(),expected.generation(),expected.directoryEpoch(),expected.storageEpoch(),expected.hashVersion(),expected.group(),expected.groupEpoch(),expected.leaseSequence(),expected.ownerIncarnation(),expected.reservationId(),expected.reservationVersion(),expected.activationId(),expected.callVersion(),expected.negotiationId(),expected.intentHash(),expected.issuedAt(),expected.expiresAt(),expected.sourceStorageEpoch(),expected.participantUntil());assertThat(proof.verify(signed,other,"c001",now)).isFalse();
    }
    @Test void actualMutualTlsExtractsPeerIdentityAndRejectsWrongDestinationAndUnauthorizedOperation()throws Exception {
        var seen=new AtomicInteger();CellRpcServer.Backend backend=(operation,c,peer,budget)->{assertThat(peer.cell()).isEqualTo("c001");assertThat(peer.role()).isEqualTo("actor");assertThat(budget).isLessThanOrEqualTo(Duration.ofSeconds(2));seen.incrementAndGet();return CompletableFuture.completedFuture(committed(c));};
        try(var server=new CellRpcServer("c002","test",0,serverTls(),new RpcAdmission(4,256*1024,4,256*1024),backend,event->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();var client=client(server.port(),"actor")){
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isTrue();
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c001"),Duration.ofSeconds(2)).toCompletableFuture().join().getErrorCode()).isEqualTo("WRONG_CELL");
            try(var gateway=client(server.port(),"gateway")){assertThat(gateway.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(2)).toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");}
            try(var outsider=client(server.port(),"outsider")){assertThat(outsider.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isFalse();}
            assertThat(seen).hasValue(1);assertThat(client.channelCount()).isEqualTo(1);
        }
    }
    @Test void oneRetryLayerPreservesOriginalRequestAndBudgetWithSeparateRelayChannels()throws Exception {
        var attempts=new AtomicInteger();var budgets=new CopyOnWriteArrayList<Duration>();var operationIds=new CopyOnWriteArrayList<String>();
        CellRpcServer.Backend backend=(op,c,peer,budget)->{budgets.add(budget);operationIds.add(c.getOperationId());if(op==CellRpcServer.Operation.RESERVE&&attempts.incrementAndGet()<3)return CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException());return CompletableFuture.completedFuture(committed(c));};
        try(var server=new CellRpcServer("c002","test",0,serverTls(),new RpcAdmission(4,256*1024,4,256*1024),backend,event->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();var client=client(server.port(),"actor")){
            var c=command("c002");assertThat(client.call(CellRpcServer.Operation.RESERVE,c,Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isTrue();assertThat(attempts).hasValue(3);assertThat(operationIds.stream().distinct().toList()).containsExactly(c.getOperationId());assertThat(budgets.get(2)).isLessThan(budgets.getFirst());
            assertThat(client.call(CellRpcServer.Operation.RELAY,c.toBuilder().setType("RelayNegotiation").build(),Duration.ofSeconds(1)).toCompletableFuture().join().getAckCommitted()).isTrue();assertThat(budgets.getLast()).isLessThanOrEqualTo(Duration.ofSeconds(1));assertThat(client.channelCount()).isEqualTo(2);
        }
    }
    @Test void coordinatorTimeoutAfterCommittedHomeWinnerKeepsAcquisitionIdentityForReconciliation(){
        var calls=new AtomicInteger();UUID operation=UUID.randomUUID();var saga=new CrossCellSaga((phase,id,budget)->{calls.incrementAndGet();if(phase==CrossCellSaga.Phase.CLAIM_HOME)return CompletableFuture.completedFuture("CLAIMED");return CompletableFuture.failedFuture(new DbOutcomeUnknownException());});
        var outcome=saga.accept(operation,Duration.ofSeconds(2)).toCompletableFuture().join();assertThat(outcome.operation()).isEqualTo(operation);assertThat(outcome.code()).isEqualTo("OUTCOME_UNKNOWN");assertThat(outcome.reconcileHomeWinner()).isTrue();assertThat(calls).hasValue(2);
    }
    @Test void trustedCaCertificateForAnotherCellCannotAuthenticateTheDestination()throws Exception {
        try(var server=new CellRpcServer("c003","test",0,serverTls(),new RpcAdmission(4,256*1024,4,256*1024),(op,c,p,b)->CompletableFuture.completedFuture(committed(c)),event->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();var client=new CellRpcClient(Map.of("c003",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),clientTls("actor"),new RpcAdmission(4,256*1024,4,256*1024))){assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c003"),Duration.ofSeconds(2)).toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");}
    }
    @Test void logicalClientTimeoutDoesNotReleaseServerCapacityBeforePhysicalBackendCompletion()throws Exception {
        var pending=new CompletableFuture<InternalReply>();var admission=new RpcAdmission(1,256*1024,4,256*1024);var entered=new CountDownLatch(1);
        try(var server=new CellRpcServer("c002","test",0,serverTls(),admission,(op,c,p,b)->{entered.countDown();return pending;},event->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();var client=client(server.port(),"actor")){
            var first=command("c002");var stage=client.call(CellRpcServer.Operation.RESERVE,first,Duration.ofMillis(500));assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();assertThat(stage.toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("OVERLOADED");pending.complete(committed(first));org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).until(()->admission.inFlight(RpcAdmission.Lane.CONTROL)==0);
        }
    }
    @Test void releaseBeforeReserveAndDuplicatedReserveCannotReopenRetainedHomeParticipation()throws Exception {
        try(var runtime=new DbTestRuntime()){
            var home=new HomeParticipationService(runtime.sql,"c001",1,r->r.grant().proof().equals("TEST_ONLY_VERIFIED"));var reservations=new UserReservationService(home);Instant now=Instant.now();UUID operation=UUID.randomUUID();
            var grant=new HomeParticipationService.Grant("c001",1,1,685,2,1,operation,now,now.plusSeconds(5),"TEST_ONLY_VERIFIED");var request=new HomeParticipationService.Request(new UserId("test-rpc-user"),new CallId(CALL),operation,"a".repeat(64),1,HomeParticipationService.Phase.RINGING,grant);
            assertThat(reservations.releaseIfCallVersion(request,null,0).toCompletableFuture().join().terminal()).isTrue();assertThat(reservations.reserveUser(request).toCompletableFuture().join().terminal()).isTrue();assertThat(reservations.reserveUser(request).toCompletableFuture().join().reservationId()).isNull();
        }
    }
    @Test void rejectedOrUnknownPhaseNeverStartsNextEffectAndUnknownWinnerRequiresReconciliation(){
        for(String code:List.of("OVERLOADED","UNAUTHORIZED","UNAVAILABLE","INVALID","OUTCOME_UNKNOWN")){
            var seen=new AtomicInteger();var saga=new CrossCellSaga((phase,op,budget)->{seen.incrementAndGet();return CompletableFuture.completedFuture(code);});
            var result=saga.accept(UUID.randomUUID(),Duration.ofSeconds(2)).toCompletableFuture().join();assertThat(seen).hasValue(1);assertThat(result.code()).isEqualTo(code);assertThat(result.reconcileHomeWinner()).isEqualTo(code.equals("OUTCOME_UNKNOWN"));
        }
    }
    @Test void productionTlsChecksDestinationIdentityBeforeAnyBackendExecution()throws Exception {
        var seen=new AtomicInteger();
        try(var server=new CellRpcServer("c003","test",0,serverTls(),new RpcAdmission(4,256*1024,4,256*1024),(op,c,p,b)->{seen.incrementAndGet();return CompletableFuture.completedFuture(committed(c));},event->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();
            var client=new CellRpcClient("test",Map.of("c003",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("actor.crt"),cert("actor.key")),new RpcAdmission(4,256*1024,4,256*1024))){
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c003"),Duration.ofMillis(500)).toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(seen).hasValue(0);
        }
    }

    @Test void separatePhysicalBackendReceiptKeepsRpcCapacityAfterLogicalUnknown()throws Exception {
        var physical=new CompletableFuture<Void>();var admission=new RpcAdmission(1,262144,4,262144);CellRpcServer.Backend backend=new CellRpcServer.Backend(){
            public CompletionStage<InternalReply> execute(CellRpcServer.Operation op,InternalCommand command,CellRpcServer.Peer peer,Duration budget){throw new AssertionError("Production ingress must use tracked lifecycle");}
            public RpcOperation<InternalReply> executeTracked(CellRpcServer.Operation op,InternalCommand command,CellRpcServer.Peer peer,Duration budget){return new RpcOperation<>(CompletableFuture.completedFuture(InternalReply.newBuilder().setOperationId(command.getOperationId()).setCallId(command.getCallId()).setErrorCode("OUTCOME_UNKNOWN").build()),physical);}
        };
        try(var server=new CellRpcServer("c002","test",0,serverTls(),admission,backend,event->CompletableFuture.failedFuture(new AssertionError())).start();var client=client(server.port(),"actor")){
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("OVERLOADED");physical.complete(null);org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).until(()->admission.inFlight(RpcAdmission.Lane.CONTROL)==0);
        }
    }
    @Test void controlDeliveryRejectsForgedCoordinatorAndMalformedDestinationBeforeForwarding()throws Exception {
        var seen=new AtomicInteger();var route=SessionIdentity.newBuilder().setUserId("delivery-user").setIssuer("TEST_ONLY").setJti("jti").setIncarnation(ByteString.copyFrom(new byte[16])).setConnectionId(ByteString.copyFrom(new byte[16])).setConnectionGeneration(1);var event=ControlEvent.newBuilder().setEventId(UUID.randomUUID().toString()).setCallId(CALL).setCallVersion(1).setAuthorityBucketId(1).setType("CALL_READY").setMetadata(ByteString.copyFromUtf8("{}" )).setDestination(route).build();
        try(var server=new CellRpcServer("c002","test",0,serverTls(),new RpcAdmission(4,262144,4,262144),(op,c,p,b)->CompletableFuture.failedFuture(new AssertionError()),c->{seen.incrementAndGet();return CompletableFuture.completedFuture(InternalReply.newBuilder().setOperationId(c.getEventId()).setCallId(c.getCallId()).setStatus("WRITE_COMPLETED").build());}).start();var client=client(server.port(),"actor")){
            assertThat(client.deliver("c002",event.toBuilder().setCallId(CALL.replace("c001","c003")).build(),Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");
            assertThat(client.deliver("c002",event.toBuilder().setDestination(route.setConnectionGeneration(0)).build(),Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("INVALID_MESSAGE");assertThat(seen).hasValue(0);
            assertThat(client.deliver("c002",event,Duration.ofSeconds(1)).toCompletableFuture().join().getStatus()).isEqualTo("WRITE_COMPLETED");assertThat(seen).hasValue(1);
        }
    }
    @Test void serverCreditCannotRetireWhenCleanupPrecedesLogicalReply()throws Exception{
        var admission=new RpcAdmission(1,262144,4,262144);var pending=new CompletableFuture<InternalReply>();var entered=new CountDownLatch(1);var firstCommand=command("c002");
        CellRpcServer.Backend backend=new CellRpcServer.Backend(){
            public CompletionStage<InternalReply> execute(CellRpcServer.Operation op,InternalCommand c,CellRpcServer.Peer peer,Duration budget){throw new AssertionError();}
            public RpcOperation<InternalReply> executeTracked(CellRpcServer.Operation op,InternalCommand c,CellRpcServer.Peer peer,Duration budget){entered.countDown();return new RpcOperation<>(pending,CompletableFuture.completedFuture(null));}
        };
        try(var server=new CellRpcServer("c002","test",0,serverTls(),admission,backend,event->CompletableFuture.failedFuture(new AssertionError())).start();var client=client(server.port(),"actor")){
            var first=client.call(CellRpcServer.Operation.RESERVE,firstCommand,Duration.ofSeconds(2));assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThat(client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(1)).toCompletableFuture().get(2,TimeUnit.SECONDS).getErrorCode()).isEqualTo("OVERLOADED");
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            pending.complete(InternalReply.newBuilder().setOperationId(firstCommand.getOperationId()).setCallId(firstCommand.getCallId()).setStatus("COMMITTED").setAckCommitted(true).build());
            assertThat(first.toCompletableFuture().get(1,TimeUnit.SECONDS).getAckCommitted()).isTrue();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).until(()->admission.inFlight(RpcAdmission.Lane.CONTROL)==0);
        }finally{pending.complete(InternalReply.getDefaultInstance());}
    }

    @Test void clientRejectsAuthenticatedReplyWithCorrectOperationButDifferentCall()throws Exception{
        var unsafe=new CellIngressGrpc.CellIngressImplBase(){
            @Override public void reserveUser(InternalCommand c,io.grpc.stub.StreamObserver<InternalReply> reply){
                reply.onNext(InternalReply.newBuilder().setOperationId(c.getOperationId()).setCallId("c001.e1.00000000-0000-0000-0000-000000000099").setAckCommitted(true).setStatus("COMMITTED").build());reply.onCompleted();
            }
        };
        var server=io.grpc.netty.NettyServerBuilder.forPort(0).sslContext(serverTls()).addService(unsafe).build().start();
        try(var client=client(server.getPort(),"actor")){
            var reply=client.call(CellRpcServer.Operation.RESERVE,command("c002"),Duration.ofSeconds(2)).toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertThat(reply.getAckCommitted()).isFalse();assertThat(reply.getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");
        }finally{server.shutdownNow().awaitTermination(2,TimeUnit.SECONDS);}
    }
    @Test void clientRejectsDeliveryReceiptWhoseEventIdentityDoesNotMatchTheOriginal()throws Exception{
        var unsafe=new CellIngressGrpc.CellIngressImplBase(){
            @Override public void deliverControlEvent(ControlEvent event,io.grpc.stub.StreamObserver<InternalReply> reply){
                reply.onNext(InternalReply.newBuilder().setOperationId(UUID.randomUUID().toString()).setCallId(event.getCallId()).setStatus("WRITE_COMPLETED").build());reply.onCompleted();
            }
        };
        var server=io.grpc.netty.NettyServerBuilder.forPort(0).sslContext(serverTls()).addService(unsafe).build().start();
        try(var client=client(server.getPort(),"actor")){
            var event=ControlEvent.newBuilder().setEventId(UUID.randomUUID().toString()).setCallId(CALL).build();
            assertThat(client.deliver("c002",event,Duration.ofSeconds(2)).toCompletableFuture().get(3,TimeUnit.SECONDS).getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");
        }finally{server.shutdownNow().awaitTermination(2,TimeUnit.SECONDS);}
    }

}
