package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.flywaydb.core.Flyway;
import java.io.File;
import java.security.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class NativeCrossCellTransportIT {
    static File cert(String name){return new File(Objects.requireNonNull(NativeCrossCellTransportIT.class.getResource("/test-only-pki/"+name)).getFile());}
    @Test void actualTypedActorsAndDestinationTlsExecuteRemoteReservationWithNativeSealedGrant()throws Exception {
        String schema="cross_home_"+UUID.randomUUID().toString().replace("-","");String url=PgFixture.PG.getJdbcUrl()+"&currentSchema="+schema;Flyway.configure().dataSource(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword()).schemas(schema).defaultSchema(schema).locations("classpath:db/migration").load().migrate();
        try(var c=DriverManager.getConnection(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());var q=c.createStatement()){q.execute("INSERT INTO cell_authority VALUES(1,'c002',1,'GROUPED',1,'ACTIVE')");}
        var kit=ActorTestKit.create();try(var callerHome=new LocalInviteAtomicIT.Fixture();var calleeHome=new DbTestRuntime(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword())){
            var caller=callerHome.sender("rpc-native-caller");var callee=new UserId("rpc-native-callee");var invite=callerHome.invite(caller,callee);var call=CallId.create("c001",1);var token=callerHome.token(call);var commands=new CallCommandService(callerHome.runtime.sql,"c001",1,c->CompletableFuture.completedFuture(new CallCommandService.Authority(call,token,1,"TEST_ONLY_VERIFIED",0,new CallCommandService.TargetHome("c002",1))),(c,s,p)->p.equals("TEST_ONLY_VERIFIED")).businessAdmission(()->true);assertThat(NativeProofSagaIT.done(commands.executeUnderAuthorityTracked(invite,new CallCommandService.Authority(call,token,1,"TEST_ONLY_VERIFIED",0,new CallCommandService.TargetHome("c002",1)),Duration.ofSeconds(2))).status()).isEqualTo("PENDING");
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));var bindings=new ProofBindings(proofs,Clock.systemUTC());var issuer=new NativeProofIssuer(proofs,Clock.systemUTC(),()->true,g->g.equals(token));
            var workflow=new CallWorkflowService(callerHome.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,s)->false);var callBackend=new CallCommandHandler(workflow,commands,()->Optional.of(token),1,u->new CallCommandService.TargetHome("c002",1),new CoordinatorGrantService(callerHome.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER"),issuer::coordinator);var callActor=kit.spawn(CallActor.create(call,callBackend,()->Optional.of(token),Clock.systemUTC()));
            var home=new HomeParticipationService(calleeHome.sql,"c002",1,bindings.homeVerifier("c002"));var userBackend=new PostgresUserBackend(new UserSnapshotService(calleeHome.sql,"c002",1),new SessionRegistryService(calleeHome.sql,"c002",1),new UserReservationService(home),new AcceptWinnerService(home,(c,r)->true),new HomeActivationService(home,(c,u)->true),new HomeProofReadService(home,(c,u)->true,(c,r)->true));var shard=kit.<org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding.ShardCommand>createTestProbe();var userActor=kit.spawn(UserActor.create(callee,1,shard.ref(),userBackend,Clock.systemUTC()));
            RpcBusinessHandler.ActorIngress actors=new RpcBusinessHandler.ActorIngress(){
                public CompletionStage<CallActor.GrantReply> grant(Request r,AuthorizationIntent a,String destination,Instant d,int bytes){return AskPattern.ask(callActor,reply->new CallActor.GrantToHome(r,a,destination,reply,d,bytes),Duration.between(Instant.now(),d),kit.scheduler());}
                public CompletionStage<UserCommand.Result> user(UserCommand.Operation op,Instant d,int bytes){return AskPattern.ask(userActor,reply->new UserCommand.Mutate(op,reply,d,bytes),Duration.between(Instant.now(),d),kit.scheduler());}
                public CompletionStage<CallCommandService.Outcome> call(CallCommand c,String p,Instant d,int bytes){throw new AssertionError();}
                public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant d,int bytes){throw new AssertionError();}
            };
            var bridge=new RpcBusinessHandler("c002",actors,bindings,proofs,u->new ProofBindings.TrustedHome("c002",1,1),r->CompletableFuture.failedFuture(new UnsupportedOperationException()),r->CompletableFuture.failedFuture(new UnsupportedOperationException()),Clock.systemUTC()).businessAdmission(()->true);
            try(var server=new CellRpcServer("c002","test",0,RpcTlsContexts.server("test","c002",cert("ca.crt"),cert("server.crt"),cert("server.key")),new RpcAdmission(4,256*1024,4,256*1024),bridge,e->CompletableFuture.failedFuture(new UnsupportedOperationException())).start();var client=new CellRpcClient("test",Map.of("c002",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("actor.crt"),cert("actor.key")),new RpcAdmission(4,256*1024,4,256*1024))){
                Instant now=Instant.now();var request=new Request(callee,call,invite.requestId().value(),invite.intentHash(),1,Phase.PREPARING,new Grant("c001",1,1,token.group(),token.epoch(),1,invite.requestId().value(),now,now.plusSeconds(5),"UNSIGNED"));var result=new AtomicReference<UserCommand.Result>();NativeSagaEffects.Planner planner=new NativeSagaEffects.Planner(){public NativeSagaEffects.Step next(CrossCellSaga.Phase phase){return new NativeSagaEffects.HomeStep("c002",new UserCommand.Reserve(request),null);}public void completed(CrossCellSaga.Phase phase,Object dto){result.set((UserCommand.Result)dto);}};
                var effects=new NativeSagaEffects(actors,client::call,planner,Clock.systemUTC());assertThat(effects.apply(CrossCellSaga.Phase.RESERVE_HOME,invite.requestId().value(),Duration.ofSeconds(2)).toCompletableFuture().join()).isEqualTo("RESERVED");assertThat(result.get().participation().call()).isEqualTo(call);assertThat(result.get().participation().reservationId()).isNotNull();assertThat(effects.apply(CrossCellSaga.Phase.RESERVE_HOME,invite.requestId().value(),Duration.ofSeconds(2)).toCompletableFuture().join()).isEqualTo("RESERVED");
                try(var c=DriverManager.getConnection(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM user_reservation")){r.next();assertThat(r.getInt(1)).isEqualTo(1);}
            }
        }finally{kit.shutdownTestKit();}
    }
}
