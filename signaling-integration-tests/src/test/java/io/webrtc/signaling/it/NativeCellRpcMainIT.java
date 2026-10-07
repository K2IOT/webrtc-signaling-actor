package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.app.runtime.NativeCellRpcEnrollment;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** Main's original native transport factory; echo business callback is explicitly TEST_ONLY. */
class NativeCellRpcMainIT {
    @ParameterizedTest @ValueSource(strings={"actor","gateway"})
    void mainCreatesOneLazyClientFromExplicitCellBoundTlsAndTopology(String plane)throws Exception {
        var tls=RpcTlsContexts.clients("test",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert(plane.equals("actor")?"server.crt":"gateway.crt"),NativeGatewayCommandIT.cert(plane.equals("actor")?"server.key":"gateway.key"));
        var created=new AtomicReference<CellRpcClient>();
        try(var server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert("server.crt"),NativeGatewayCommandIT.cert("server.key")),new RpcAdmission(8,1048576,8,1048576),(operation,command,peer,budget)->CompletableFuture.completedFuture(InternalReply.newBuilder().setOperationId(command.getOperationId()).setCallId(command.getCallId()).setStatus("WRITE_COMPLETED").build()),event->CompletableFuture.failedFuture(new AssertionError())).start()){
            var enrollment=new NativeCellRpcEnrollment("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),tls,new RpcAdmission(8,1048576,8,1048576));
            var defaults=new org.springframework.boot.env.YamlPropertySourceLoader().load("TEST_ONLY_defaults",new org.springframework.core.io.FileSystemResource("../config/production-defaults.yaml"));
            new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
                .withBean(NativeCellRpcEnrollment.class,()->enrollment)
                .withInitializer(context->{defaults.forEach(value->context.getEnvironment().getPropertySources().addLast(value));context.getEnvironment().setActiveProfiles(plane);})
                .withPropertyValues("signaling.identity.issuer=TEST_ONLY_ISSUER","signaling.identity.audience=TEST_ONLY_AUDIENCE")
                .run(context->{
                    assertThat(context).hasNotFailed().hasSingleBean(CellRpcClient.class);
                    var client=context.getBean(CellRpcClient.class);created.set(client);
                    assertThat(client.channelCount()).isZero();
                    var command=InternalCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setOperationId(UUID.randomUUID().toString()).setType("INVITE").setCallId("c001.e1."+UUID.randomUUID()).setPayloadHash(com.google.protobuf.ByteString.copyFrom(new byte[32])).setRemainingBudgetMs(2000).build();
                    var work=client.callTracked(CellRpcServer.Operation.EXECUTE,command,Duration.ofSeconds(2));
                    var reply=work.logical().toCompletableFuture().get(3,TimeUnit.SECONDS);assertThat(reply.getErrorCode()).isEmpty();assertThat(reply.getStatus()).isEqualTo("WRITE_COMPLETED");assertThat(reply.getAckCommitted()).isFalse();
                    work.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);assertThat(client.channelCount()).isEqualTo(1);

                });
            // A partial factory context has no plane lifecycle: the fixture retains and retires the original owner.
            var probe=InternalCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setOperationId(UUID.randomUUID().toString()).setType("INVITE").setCallId("c001.e1."+UUID.randomUUID()).setPayloadHash(com.google.protobuf.ByteString.copyFrom(new byte[32])).setRemainingBudgetMs(2000).build();
            assertThat(created.get().call(CellRpcServer.Operation.EXECUTE,probe,Duration.ofSeconds(2)).toCompletableFuture().get(3,TimeUnit.SECONDS).getStatus()).isEqualTo("WRITE_COMPLETED");
        }finally{if(created.get()!=null)created.get().drain().toCompletableFuture().get(4,TimeUnit.SECONDS);}
    }
}
