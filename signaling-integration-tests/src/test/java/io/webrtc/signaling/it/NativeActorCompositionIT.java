package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.lease.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.typed.*;
import org.apache.pekko.cluster.MemberStatus;
import com.typesafe.config.*;
import java.time.*;
import java.util.*;
import java.security.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Actual native backends and EntityRefs across four test-only loopback TCP members; external TLS/AZ admission remains separate. */
class NativeActorCompositionIT {
    static Config config(String zone){return ConfigFactory.parseString("""
        signaling.cell-id="c001"
        signaling.cluster-fingerprint="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        pekko.remote.artery.transport=tcp
        pekko.remote.artery.canonical.hostname="127.0.0.1"
        pekko.remote.artery.canonical.port=0
        pekko.management.http.hostname="127.0.0.1"
        pekko.discovery.kubernetes-api.pod-namespace="test-only-c001"
        pekko.discovery.kubernetes-api.pod-label-selector="app=webrtc-signaling,plane=actor,cell=c001"
        pekko.management.cluster.bootstrap.contact-point-discovery.service-name="signaling-c001"
        pekko.cluster.roles=["signaling-actor","az-%s"]
        pekko.loglevel=WARNING
        pekko.cluster.jmx.multi-mbeans-in-same-jvm=on
        """.formatted(zone)).withFallback(ShardingBootstrap.baseConfig()).resolve();}
    @Test void realEntityRefsSelectNativeRootsAndCommitThroughInstalledBackendFactories()throws Exception {
        var systems=new ArrayList<ActorSystem<Void>>();var compositions=new ArrayList<NativeActorComposition>();
        var f=new LocalInviteAtomicIT.Fixture();
        try(var verifier=new BoundedTokenVerifier((token,now)->{throw new IllegalArgumentException("TEST_ONLY_UNUSED_AUTH");},1,8,Duration.ofSeconds(1))){
            var caller=f.sender("composition-caller");var callee=f.sender("composition-callee");
            var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",key.getPrivate(),Map.of("c001/test",key.getPublic()));
            for(var zone:List.of("a","a","b","c")){
                var system=ActorSystem.<Void>create(Behaviors.empty(),"native-composition-c001",config(zone));systems.add(system);
                var readiness=new ClusterReadiness();readiness.update(new ClusterReadiness.Snapshot(false,false,true,true,true,true,0,0,false));
                var inputs=new NativeActorComposition.Inputs(f.runtime.sql,"c001",1,1,UUID.randomUUID(),proofs,u->new ProofBindings.TrustedHome("c001",1,1),(c,p)->true,(c,u)->true,(c,from,to)->false,verifier,CallAuthorizationPolicy.denyAll(),Clock.systemUTC(),()->true);
                compositions.add(new NativeActorComposition(system,inputs,readiness));
            }
            var seed=Cluster.get(systems.getFirst()).selfMember().address();for(var system:systems)Cluster.get(system).manager().tell(new JoinSeedNodes(List.of(seed)));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).until(()->systems.stream().allMatch(s->Cluster.get(s).selfMember().status().equals(MemberStatus.up())));
            for(var composition:compositions){composition.readiness().update(new ClusterReadiness.Snapshot(true,false,true,true,true,true,4,3,false));composition.register();}
            var cell=compositions.getLast();var call=CallId.create("c001",1);var original=f.invite(caller,callee.userId());
            var command=new CallCommand(original.type(),caller,original.requestId(),call,original.scope(),callee.userId(),null,null,"{}",original.intentHash());
            var route=SessionAuthReadIT.route(f,caller);var registry=new SessionRegistryService(f.runtime.sql,"c001",1,(c,p)->true);var auth=CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,SessionAuthReadIT.principal(route),1,Duration.ofSeconds(2)));var signed=proofs.sessionProofs().issue(auth,command);
            CallCommandService.Outcome result=null;long end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
            while(System.nanoTime()<end){auth=CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,SessionAuthReadIT.principal(route),1,Duration.ofSeconds(2)));signed=proofs.sessionProofs().issue(auth,command);var operation=cell.ingress().callTracked(command,signed,Instant.now().plusSeconds(2),RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command,signed)).length);try{result=operation.logical().toCompletableFuture().join();}catch(CompletionException unknown){assertThat(unknown.getCause()).isInstanceOfAny(org.apache.pekko.pattern.AskTimeoutException.class,DbOutcomeUnknownException.class,TimeoutException.class);}finally{operation.physicalCompletion().toCompletableFuture().get(8,TimeUnit.SECONDS);}if(result!=null&&result.status().equals("FINAL"))break;java.util.concurrent.locks.LockSupport.parkNanos(20_000_000);}
            assertThat(result).isNotNull();assertThat(result.status()).isEqualTo("FINAL");assertThat(result.state()).isEqualTo("RINGING");
            var holders=compositions.stream().filter(c->PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(c.system()),HomeParticipationService.group(call)).isPresent()).toList();assertThat(holders).hasSize(1);
            var token=PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(holders.getFirst().system()),HomeParticipationService.group(call)).orElseThrow().token();
            assertThat(token.node()).contains("#");assertThat(token.incarnation()).isNotEqualTo(f.incarnation);
            var queryAction=new HomeParticipationService.AuthorizationIntent("QUERY",null,0,null,0,null,null);var now=Instant.now();
            var template=new HomeParticipationService.Request(caller.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,UUID.randomUUID(),now,now.plusSeconds(5),"UNSIGNED"));
            var grant=cell.ingress().grant(template,queryAction,"c001",Instant.now().plusSeconds(2),RpcBusinessHandler.encode(template).length).toCompletableFuture().join();assertThat(grant.code()).isEqualTo("GRANTED");assertThat(new ProofBindings(proofs,Clock.systemUTC()).homeVerifier("c001").verify(grant.signedRequest(),queryAction)).isTrue();
        }finally{for(var system:systems)system.terminate();for(var system:systems)system.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);for(var composition:compositions)composition.drainRoots().toCompletableFuture().get(8,TimeUnit.SECONDS);f.close();}
    }
}
