package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.grpc.*;
import io.grpc.netty.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import com.google.protobuf.ByteString;
import java.io.File;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
/** Genuine TLS1.3 transport, TEST_ONLY native backend receipts, never delivery/capacity evidence. */
class GatewayRelayRpcIT {
    File cert(String name){return new File(Objects.requireNonNull(getClass().getResource("/test-only-pki/relay/"+name)).getFile());}
    RelayDelivery delivery(){var call=CallId.create("c001",1);var sender=new AuthenticatedSession(new UserId("TEST_ONLY_SENDER"),new SessionKey("TEST_ONLY","sender"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());var recipient=new AuthenticatedSession(new UserId("TEST_ONLY_RECIPIENT"),new SessionKey("TEST_ONLY","recipient"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());return new RelayDelivery(new CallCommand(SignalEnvelope.Type.OFFER,sender,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,new NegotiationId(17),new IceGeneration(29),"{\"sdp\":\""+"x".repeat(16384)+"\"}","a".repeat(64)),recipient,7,Instant.now().plusSeconds(3));}
    GatewayRelayRequest wire(RelayDelivery delivery,UUID boot){return GatewayRelayRequest.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setGatewayId("gw-1").setBootId(ByteString.copyFrom(java.nio.ByteBuffer.allocate(16).putLong(boot.getMostSignificantBits()).putLong(boot.getLeastSignificantBits()).array())).setOperationId(delivery.command().requestId().value().toString()).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(delivery))).setRemainingBudgetMs(1000).build();}
    ManagedChannel channel(int port,String client)throws Exception {return NettyChannelBuilder.forAddress("localhost",port).overrideAuthority("localhost").sslContext(RpcTlsContexts.gatewayClients("test",cert("ca.crt"),cert(client+".crt"),cert(client+".key")).context("c001","gw-1")).disableRetry().build();}
    @Test void actualTlsRelayCarriesLargeSdpAndHoldsServerCreditAfterLogicalWriteReceipt()throws Exception {
        var boot=UUID.randomUUID();var delivery=delivery();var physical=new CompletableFuture<Void>();var admission=new RpcAdmission(1,98304,1,98304);var seen=new AtomicReference<CellRpcServer.Peer>();
        var server=new GatewayRelayRpcServer("c001","gw-1",boot,"test",0,RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key")),admission,(d,p,b)->{assertThat(d).isEqualTo(delivery);assertThat(b).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(1));seen.set(p);return new RpcOperation<>(CompletableFuture.completedFuture(new RelayWriteReceipt(d.command().callId(),d.command().requestId(),d.command().type(),7,17,29)),physical);}).start();
        var channel=channel(server.port(),"actor");
        try{var reply=GatewayRelayIngressGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS).deliverRelay(wire(delivery,boot));assertThat(reply.getStatus()).isEqualTo("WRITE_COMPLETED");assertThat(reply.getAckCommitted()).isFalse();assertThat(reply.getCallVersion()).isEqualTo(7);assertThat(seen.get()).isEqualTo(new CellRpcServer.Peer("c001","actor","relay-actor"));assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();var drain=server.drain();assertThat(drain.toCompletableFuture()).isNotDone();physical.complete(null);drain.toCompletableFuture().get(3,TimeUnit.SECONDS);assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();}
        finally{physical.complete(null);channel.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);server.close();}
    }
    @Test void wrongBootReboundOperationAndTrailingPayloadNeverReachBackend()throws Exception {
        var boot=UUID.randomUUID();var delivery=delivery();var calls=new AtomicInteger();var admission=new RpcAdmission(1,98304,1,98304);
        var server=new GatewayRelayRpcServer("c001","gw-1",boot,"test",0,RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key")),admission,(d,p,b)->{calls.incrementAndGet();throw new AssertionError("Must reject before backend");}).start();var channel=channel(server.port(),"actor");
        try{var stub=GatewayRelayIngressGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS);var valid=wire(delivery,boot);for(var request:List.of(wire(delivery,UUID.randomUUID()),valid.toBuilder().setOperationId(UUID.randomUUID().toString()).build(),valid.toBuilder().setPayload(ByteString.copyFromUtf8(valid.getPayload().toStringUtf8()+" {} ")).build(),valid.toBuilder().setDestinationCell("c002").build(),valid.toBuilder().setGatewayId("other").build()))assertThat(stub.deliverRelay(request).getErrorCode()).isNotEmpty();assertThat(calls.get()).isZero();assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();}
        finally{channel.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);server.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);}
    }
    @Test void originalRelayDeadlineDoesNotCancelNativeWriteOrReleaseItsCredit()throws Exception {
        var boot=UUID.randomUUID();var delivery=delivery();var logical=new CompletableFuture<RelayWriteReceipt>();var physical=new CompletableFuture<Void>();var admission=new RpcAdmission(1,98304,1,98304);var entered=new CountDownLatch(1);
        var server=new GatewayRelayRpcServer("c001","gw-1",boot,"test",0,RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key")),admission,(d,p,b)->{entered.countDown();return new RpcOperation<>(logical,physical);}).start();var channel=channel(server.port(),"actor");
        try{var stub=GatewayRelayIngressGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS);assertThat(stub.deliverRelay(wire(delivery,UUID.randomUUID())).getErrorCode()).isNotEmpty();
            var reply=stub.deliverRelay(wire(delivery,boot).toBuilder().setRemainingBudgetMs(40).build());assertThat(reply.getErrorCode()).isEqualTo("RESYNC_REQUIRED");assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();assertThat(logical).isNotDone();assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);var settled=server.settleAdmitted();assertThat(settled.toCompletableFuture()).isNotDone();physical.complete(null);settled.toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();assertThat(logical).isNotDone();
        }finally{logical.completeExceptionally(new IllegalStateException("TEST_ONLY_CLEANUP"));physical.complete(null);channel.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);server.close();}
    }

}
