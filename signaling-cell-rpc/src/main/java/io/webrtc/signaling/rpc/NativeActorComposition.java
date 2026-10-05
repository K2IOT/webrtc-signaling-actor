package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.lease.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** One actor pod: installed native roots, genuine User/Call backends, EntityRef ingress and RPC producers. */
public final class NativeActorComposition {
    public record Inputs(SqlTransactions sql,String cell,long storageEpoch,long routingEpoch,UUID podUid,
            HomeAuthorizationProof proofs,Function<UserId,ProofBindings.TrustedHome> homes,
            SessionRegistryService.NativeSecurityPolicy sessionSecurity,HomeProofReadService.SecurityPolicy homeSecurity,
            HomeParticipationService.EpochAdoptionVerifier epochAdoption,BoundedTokenVerifier tokenVerifier,
            CallAuthorizationPolicy callPolicy,Clock clock,BooleanSupplier trustedClock,AcceptWinnerService.RouteSecurityPolicy routeSecurity){
        public Inputs {
            if(cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}")||storageEpoch<1||routingEpoch<1)throw new IllegalArgumentException("Invalid native actor identity");
            Objects.requireNonNull(sql);Objects.requireNonNull(podUid);Objects.requireNonNull(proofs);Objects.requireNonNull(homes);
            Objects.requireNonNull(routeSecurity);Objects.requireNonNull(sessionSecurity);Objects.requireNonNull(homeSecurity);Objects.requireNonNull(epochAdoption);
            Objects.requireNonNull(tokenVerifier);Objects.requireNonNull(callPolicy);Objects.requireNonNull(clock);Objects.requireNonNull(trustedClock);
            if(!cell.equals(proofs.sourceCell()))throw new IllegalArgumentException("Signer cell differs from native authority");
        }
    }
    private final ActorSystem<?> system;private final Inputs inputs;private final ClusterReadiness readiness;
    private final ShardedActorIngress ingress;private final NativeProofIssuer issuer;private final ProofBindings bindings;
    private final SessionRegistryService sessions;private final PostgresUserBackend users;private final CallCommandService commands;
    private final CallWorkflowService workflow;private final CoordinatorGrantService grants;
    private boolean registered;private volatile NativeRelayProducer relayProducer;
    public NativeActorComposition(ActorSystem<?> system,Inputs inputs,ClusterReadiness readiness){
        this.system=Objects.requireNonNull(system);this.inputs=Objects.requireNonNull(inputs);this.readiness=Objects.requireNonNull(readiness);
        var classic=Adapter.toClassic(system);var roots=new GroupOwnerRepository(inputs.sql(),inputs.cell(),inputs.storageEpoch());
        PostgresShardLeaseProvider.install(classic,roots,inputs.cell(),inputs.storageEpoch(),inputs.podUid(),inputs.trustedClock());
        bindings=new ProofBindings(inputs.proofs(),inputs.clock());ingress=new ShardedActorIngress(system,inputs.clock());
        var home=new HomeParticipationService(inputs.sql(),inputs.cell(),inputs.storageEpoch(),bindings.homeVerifier(inputs.cell()),inputs.epochAdoption());
        sessions=new SessionRegistryService(inputs.sql(),inputs.cell(),inputs.storageEpoch(),inputs.sessionSecurity());
        users=new PostgresUserBackend(new UserSnapshotService(inputs.sql(),inputs.cell(),inputs.storageEpoch()),sessions,new UserReservationService(home),new AcceptWinnerService(home,inputs.routeSecurity()),new HomeActivationService(home,inputs.homeSecurity()),new HomeProofReadService(home,inputs.homeSecurity(),inputs.routeSecurity()));
        commands=new CallCommandService(inputs.sql(),inputs.cell(),inputs.storageEpoch(),inputs.routingEpoch(),c->{throw new IllegalStateException("Commands require hosting EntityRef authority");},bindings.commandVerifier(inputs.cell(),inputs.homes()),bindings.negotiationVerifier(inputs.cell(),inputs.homes())).businessAdmission(readiness::businessReady);
        String owner=PostgresShardLeaseProvider.ownerNode(classic);
        workflow=new CallWorkflowService(inputs.sql(),inputs.cell(),inputs.storageEpoch(),owner,bindings.workflowVerifier(inputs.cell(),inputs.homes()));
        grants=new CoordinatorGrantService(inputs.sql(),inputs.cell(),inputs.storageEpoch(),owner);
        issuer=new NativeProofIssuer(inputs.proofs(),inputs.clock(),inputs.trustedClock(),token->local(token.group()).filter(token::equals).isPresent());
    }
    private Optional<AuthoritySql.GroupToken> local(int group){
        if(!inputs.trustedClock().getAsBoolean()||!readiness.safetyReady())return Optional.empty();
        return PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(system),group).map(GroupOwnerRepository.Grant::token);
    }
    private ProofBindings.TrustedHome localHome(UserId user){var home=Objects.requireNonNull(inputs.homes().apply(user));if(!inputs.cell().equals(home.cell())||home.storageEpoch()!=inputs.storageEpoch())throw new AuthoritySql.FencedException();return home;}
    public synchronized ShardingBootstrap.Regions register(){
        if(registered)throw new IllegalStateException("Native regions already registered");
        var regions=ShardingBootstrap.registerBoth(system,entity->{var user=new UserId(entity.getEntityId());return UserActor.create(user,localHome(user).directoryEpoch(),entity.getShard(),users,inputs.clock());},UserCommand.Stop.INSTANCE,entity->{
            var call=new CallId(entity.getEntityId());int group=HomeParticipationService.group(call);Supplier<Optional<AuthoritySql.GroupToken>> gate=()->local(group);
            var backend=new CallCommandHandler(workflow,commands,gate,user->{var home=Objects.requireNonNull(inputs.homes().apply(user));return new CallCommandService.TargetHome(home.cell(),home.directoryEpoch());},grants,issuer::coordinator,
                command->command.type()==SignalEnvelope.Type.INVITE?localHome(command.sender().userId()).directoryEpoch():0L);
            return CallActor.create(call,backend,gate,inputs.clock(),entity.getShard(),changed->{var producer=relayProducer;if(producer!=null)producer.invalidate(changed);});
        },CallActor.Stop.INSTANCE,readiness);registered=true;return regions;
    }
    public RpcBusinessHandler backend(NativeSagaEffects.Network network,Function<RpcBusinessHandler.RelayRequest,CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply>> relay){
        var handler=new RpcBusinessHandler(inputs.cell(),ingress,bindings,inputs.proofs(),inputs.homes(),read->{throw new IllegalStateException("Native reads are mandatory");},relay,inputs.clock(),issuer).nativeReads(new NativeSnapshotReads(commands)).businessAdmission(readiness::businessReady);
        var homeProofs=new NativeHomeProofClient(ingress,Objects.requireNonNull(network),inputs.clock());
        return handler.nativeSetup(new NativeSetupCommandExecutor(commands,homeProofs,ingress,network,inputs.homes(),inputs.cell(),inputs.storageEpoch(),inputs.clock()))
            .nativeCritical(new NativeCriticalCommandExecutor(commands,homeProofs,ingress,inputs.homes(),inputs.cell(),inputs.storageEpoch(),inputs.clock()));
    }
    /** Install at the actual hosting pod; a nonhosting process cannot refresh relay authority. */
    public NativeRelayAuthorization relayAuthorization(NativeSagaEffects.Network network){
        return new NativeRelayAuthorization(commands,new NativeHomeProofClient(ingress,Objects.requireNonNull(network),inputs.clock()),inputs.proofs(),inputs.homes(),
            call->local(HomeParticipationService.group(call)),inputs.clock(),()->inputs.trustedClock().getAsBoolean()&&readiness.safetyReady());
    }
    /** Caller owns lifecycle; install with backend.nativeRelay before opening the RPC listener. */
    public synchronized NativeRelayProducer relayProducer(NativeSagaEffects.Network network,int capacity,
            io.webrtc.signaling.actors.relay.RelayBufferBudget memory,GatewayRelayRpcClient gateway){
        if(relayProducer!=null)throw new IllegalStateException("Native relay producer already created");Objects.requireNonNull(gateway);Objects.requireNonNull(memory);
        BooleanSupplier trusted=()->inputs.trustedClock().getAsBoolean()&&readiness.safetyReady();
        Function<CallId,Optional<AuthoritySql.GroupToken>> owner=call->local(HomeParticipationService.group(call));
        var rounds=new NativeRelayRoundCache(capacity,Math.min(capacity,64),relayAuthorization(network),System::nanoTime,trusted,owner);
        relayProducer=new NativeRelayProducer(rounds,capacity,System::nanoTime,trusted,owner,memory,gateway::send);return relayProducer;
    }
    /** Transfers gateway ownership to the installed listener; bind only after native region registration. */
    public synchronized NativeActorRpcIngress rpcIngress(String environment,int port,io.netty.handler.ssl.SslContext tls,
            RpcAdmission admission,NativeSagaEffects.Network network,int capacity,
            io.webrtc.signaling.actors.relay.RelayBufferBudget memory,GatewayRelayRpcClient gateway,
            BiPredicate<CellRpcServer.Peer,NativeSessionHandler.GatewayIdentity> gatewayWorkloads,
            Function<io.webrtc.signaling.protocol.internal.ControlEvent,CompletionStage<io.webrtc.signaling.protocol.internal.InternalReply>> deliver){
        if(!registered)throw new IllegalStateException("Native actor regions not installed");
        if(environment==null||!environment.matches("[a-z0-9-]{1,32}")||port<0||port>65535)throw new IllegalArgumentException("Invalid native listener identity");
        Objects.requireNonNull(tls);Objects.requireNonNull(admission);Objects.requireNonNull(network);Objects.requireNonNull(memory);Objects.requireNonNull(gateway);Objects.requireNonNull(gatewayWorkloads);Objects.requireNonNull(deliver);
        var sessions=new NativeSessionHandler(inputs.cell(),inputs.storageEpoch(),gatewayWorkloads,sessionOperations());
        var backend=backend(network,request->CompletableFuture.failedFuture(new IllegalStateException("Native relay installation required")));
        var producer=relayProducer(network,capacity,memory,gateway);backend.nativeRelay(producer);
        var server=new CellRpcServer(inputs.cell(),environment,port,tls,admission,backend,deliver).sessions(sessions);
        return new NativeActorRpcIngress(server,producer,gateway);
    }
    public NativeSessionOperations sessionOperations(){return new NativeSessionOperations(sessions,inputs.tokenVerifier(),inputs.clock(),inputs.proofs().sessionProofs(),inputs.proofs().relaySessionProofs(),inputs.trustedClock(),inputs.callPolicy());}
    public void shedNewAcquisition(){PostgresShardLeaseProvider.shedNewAcquisition(Adapter.toClassic(system));}
    /** Invoke after framework handoff/leave, while the native database remains available. */
    public CompletionStage<Void> drainRoots(){return PostgresShardLeaseProvider.drain(Adapter.toClassic(system));}
    public ActorSystem<?> system(){return system;}
    public ClusterReadiness readiness(){return readiness;}
    public ShardedActorIngress ingress(){return ingress;}
}
