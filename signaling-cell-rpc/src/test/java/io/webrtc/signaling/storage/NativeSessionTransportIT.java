package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import com.google.protobuf.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class NativeSessionTransportIT {
    static File cert(String name){return new File(Objects.requireNonNull(NativeSessionTransportIT.class.getResource("/test-only-pki/session/"+name)).getFile());}
    static SessionCommand command(NativeSessionHandler.Request request){return SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setOperationId(request.operation().toString()).setType(request.type()).setRemainingBudgetMs(2000).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();}
    @Test void actualWorkloadTlsExecutesPrimaryBootAndIndependentlyVerifiedSessionRegistration()throws Exception {
        var verified=new AtomicInteger();var principal=new AuthPrincipal(new UserId("session-rpc-"+UUID.randomUUID()),new SessionKey("TEST_ONLY",UUID.randomUUID().toString()),Instant.now().plusSeconds(600),Instant.now(),"TEST_ONLY",1);
        try(var runtime=new DbTestRuntime();var verifier=new BoundedTokenVerifier((token,now)->{assertThat(token).isEqualTo("TEST_ONLY_ORIGINAL_TOKEN");assertThat(Thread.currentThread().getName()).startsWith("signal-auth-");verified.incrementAndGet();return principal;},1,4,Duration.ofSeconds(1))){
            var proofKeys=java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var homeProofs=new HomeAuthorizationProof("c001","test",proofKeys.getPrivate(),Map.of("c001/test",proofKeys.getPublic()));
            var operations=new NativeSessionOperations(new SessionRegistryService(runtime.sql,"c001",1,(connection,p)->p.equals(principal)),verifier,Clock.systemUTC(),homeProofs.sessionProofs(),()->true);var handler=new NativeSessionHandler("c001",1,(peer,gateway)->gateway.gatewayId().equals("gw-1"),operations);
            try(var server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",cert("ca.crt"),cert("server.crt"),cert("server.key")),new RpcAdmission(4,256*1024,4,256*1024),(op,c,p,b)->CompletableFuture.failedFuture(new AssertionError()),event->CompletableFuture.failedFuture(new AssertionError())).sessions(handler).start();
                var client=new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key")),new RpcAdmission(4,256*1024,4,256*1024))){
                var gateway=new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c001",1,"TEST_ONLY_REGION");var start=new NativeSessionHandler.Request("BOOT_START",gateway,null,null,null,1,1,UUID.randomUUID());assertThat(client.session(command(start),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isTrue();
                var register=new NativeSessionHandler.Request("REGISTER",gateway,"TEST_ONLY_ORIGINAL_TOKEN",null,UUID.randomUUID(),1,0,UUID.randomUUID());var reply=client.session(command(register),Duration.ofSeconds(2)).toCompletableFuture().join();assertThat(reply.getAckCommitted()).isTrue();var route=new ObjectMapper().findAndRegisterModules().readValue(reply.getResult().toByteArray(),SessionRepository.Route.class);assertThat(route.key()).isEqualTo(principal.key());assertThat(route.connectionId()).isEqualTo(register.connection());assertThat(verified).hasValue(1);
                var bound=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),route.connectionGeneration(),route.connectionId());var call=CallId.create("c001",1);var readCommand=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.SYNC_CALL,bound,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","a".repeat(64));
                var read=new NativeSessionHandler.Request("READ_PROOF",gateway,"TEST_ONLY_ORIGINAL_TOKEN",route,null,1,0,readCommand.requestId().value(),readCommand);var proofReply=client.session(command(read),Duration.ofSeconds(2)).toCompletableFuture().join();assertThat(proofReply.getStatus()).isEqualTo("READ");assertThat(proofReply.getAckCommitted()).isFalse();String sessionProof=new ObjectMapper().readValue(proofReply.getResult().toByteArray(),String.class);assertThat(homeProofs.sessionProofs().verify(sessionProof,readCommand,new ProofBindings.TrustedHome("c001",1,1),Instant.now())).isTrue();assertThat(verified).hasValue(2);
                var wrong=new NativeSessionHandler.Request("REGISTER",new NativeSessionHandler.GatewayIdentity("gw-other",gateway.bootId(),"c001",1,"TEST_ONLY_REGION"),"TEST_ONLY_ORIGINAL_TOKEN",null,UUID.randomUUID(),1,0,UUID.randomUUID());assertThat(client.session(command(wrong),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(verified).hasValue(2);
                var close=new NativeSessionHandler.Request("CLOSE",gateway,null,route,null,1,0,UUID.randomUUID());assertThat(client.session(command(close),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isTrue();assertThat(new SessionRegistryService(runtime.sql,"c001",1).lookupLiveRoutes(principal.userId(),1).toCompletableFuture().join()).isEmpty();
            }
        }
    }
}
