package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Participant;
import java.time.*;
import java.util.*;
import java.util.function.*;
/** Signs committed primary projections. Slow reads and retries never reset the original authority deadline. */
public final class NativeProofIssuer {
    private final HomeAuthorizationProof proofs;private final Clock clock;private final BooleanSupplier trustedClock;private final Predicate<AuthoritySql.GroupToken> currentLocalGroup;
    public NativeProofIssuer(HomeAuthorizationProof proofs,Clock clock,BooleanSupplier trustedClock,Predicate<AuthoritySql.GroupToken> currentLocalGroup){this.proofs=Objects.requireNonNull(proofs);this.clock=Objects.requireNonNull(clock);this.trustedClock=Objects.requireNonNull(trustedClock);this.currentLocalGroup=Objects.requireNonNull(currentLocalGroup);}
    public Request coordinator(Request request,String destination,CoordinatorGrantService.Issued issued){
        var token=issued.token();Instant now=clock.instant();if(!trustedClock.getAsBoolean()||!currentLocalGroup.test(token)||!proofs.sourceCell().equals(token.cell())||!request.call().equals(issued.snapshot().callId())||!issued.expiresAt().isAfter(now)||issued.checkedAt().isAfter(now.plusSeconds(1))||issued.expiresAt().isAfter(issued.checkedAt().plusSeconds(5)))throw new AuthoritySql.FencedException();
        var claims=new HomeAuthorizationProof.Claims(1,"COORDINATOR_GRANT",token.cell(),destination,request.grant().operation(),request.call(),request.user(),null,null,0,request.directoryEpoch(),token.storageEpoch(),token.hashVersion(),token.group(),token.epoch(),issued.sequence(),token.incarnation(),null,0,null,issued.snapshot().version(),0,issued.intentHash(),issued.checkedAt(),issued.expiresAt(),token.storageEpoch(),null);
        var grant=new Grant(token.cell(),token.storageEpoch(),token.hashVersion(),token.group(),token.epoch(),issued.sequence(),claims.operation(),claims.issuedAt(),claims.expiresAt(),proofs.issue(claims),claims.callVersion());
        return new Request(request.user(),request.call(),request.acquireOperation(),request.payloadHash(),request.directoryEpoch(),request.phase(),grant);
    }
    public String home(Request request,HomeProofReadService.View view,CallWorkflowService.Transition transition,String purpose,Participant participant){
        Instant now=clock.instant();var bindings=new ProofBindings(proofs,clock);var query=new AuthorizationIntent("QUERY",null,0,null,0,null,null);
        if(!trustedClock.getAsBoolean()||!proofs.sourceCell().equals(view.sourceCell())||!bindings.homeVerifier(view.sourceCell()).verify(request,query))throw new AuthoritySql.FencedException();
        var grant=proofs.decode(request.grant().proof(),request.grant().cell(),now).orElseThrow(AuthoritySql.FencedException::new);var token=transition.group();var p=view.participation();
        if(!request.call().equals(transition.call())||!request.call().equals(p.call())||!request.user().equals(p.user())||!request.acquireOperation().equals(p.acquireOperation())||!request.payloadHash().equals(p.payloadHash())||request.directoryEpoch()!=view.directoryEpoch()||grant.callVersion()!=transition.expectedVersion()||token.epoch()!=grant.groupEpoch()||token.group()!=grant.group()||token.storageEpoch()!=grant.storageEpoch()||token.hashVersion()!=grant.hashVersion()||!token.incarnation().equals(grant.ownerIncarnation())||p.terminal()||p.highestGroupEpoch()>token.epoch()||p.reservationId()==null||p.version()<1||p.leaseUntil()==null||!p.leaseUntil().isAfter(now.plusSeconds(5)))throw new AuthoritySql.FencedException();
        Instant expires=min(view.checkedAt().plusSeconds(5),grant.expiresAt());Instant until=p.leaseUntil();
        if(view.checkedAt().isAfter(now.plusSeconds(1))||!expires.isAfter(now))throw new AuthoritySql.FencedException();
        if(purpose.equals("TARGET_ROUTE")){
            if(transition.step()!=CallWorkflowService.Step.RING||participant!=null||transition.offered().isEmpty()||transition.offered().stream().anyMatch(offered->route(view,offered)==null))throw new AuthoritySql.FencedException();
        }else{
            var route=participant==null?null:route(view,participant);if(route==null&&purpose.equals("WINNER")&&transition.step()==CallWorkflowService.Step.ACCEPT&&participant!=null&&p.winner()!=null&&p.winner().key().equals(participant.key())&&p.winner().incarnation().equals(participant.incarnation())&&p.winner().generation()==participant.generation())route=view.currentRoutes().stream().filter(current->current.user().equals(participant.user())&&current.key().equals(participant.key())&&current.incarnation().equals(participant.incarnation())&&current.connectionGeneration()>=participant.generation()).findFirst().orElse(null);if(route==null)throw new AuthoritySql.FencedException();until=min(until,route.tokenExpiresAt());expires=min(expires,route.tokenExpiresAt());
            if(purpose.equals("WINNER")&&transition.step()==CallWorkflowService.Step.ACCEPT&&(!transition.operation().equals(view.winnerOperation())||view.winnerCallVersion()!=transition.expectedVersion()))throw new AuthoritySql.FencedException();
            if(purpose.equals("WINNER")&&(p.winner()==null||!p.winner().key().equals(participant.key())||!p.winner().incarnation().equals(participant.incarnation())||(transition.step()==CallWorkflowService.Step.ACTIVATE?p.winner().generation()>participant.generation():p.winner().generation()!=participant.generation())))throw new AuthoritySql.FencedException();
            if(purpose.equals("ACTIVE")&&(!p.phase().equals("ACTIVE")||view.activationId()==null||!view.activationId().equals(transition.activationId())||view.activationCallVersion()<1||view.activationCallVersion()>transition.expectedVersion()))throw new AuthoritySql.FencedException();
            if(!Set.of("SESSION","WINNER","ACTIVE").contains(purpose))throw new AuthoritySql.FencedException();
        }
        if(!until.isAfter(now.plusSeconds(5))||!expires.isAfter(now))throw new AuthoritySql.FencedException();
        return proofs.issue(new HomeAuthorizationProof.Claims(1,purpose,view.sourceCell(),request.call().coordinatorCell(),transition.operation(),request.call(),request.user(),participant==null?null:participant.key(),participant==null?null:participant.incarnation(),participant==null?0:participant.generation(),view.directoryEpoch(),token.storageEpoch(),token.hashVersion(),token.group(),token.epoch(),grant.leaseSequence(),token.incarnation(),p.reservationId(),p.version(),view.activationId(),transition.expectedVersion(),0,ProofBindings.workflowIntent(transition),view.checkedAt(),expires,view.sourceStorageEpoch(),until));
    }
    /** Command-scoped ACTIVE proof, produced only from a freshly guarded home projection. */
    public String homeCommand(Request request,HomeProofReadService.View view,io.webrtc.signaling.protocol.CallCommand command,CallSnapshotRepository.Snapshot snapshot,AuthoritySql.GroupToken group,Participant participant){
        if(command.callId()==null||!command.callId().equals(snapshot.callId())||!request.grant().operation().equals(command.requestId().value())||snapshot.activationId()==null||!Set.of("CONNECTING","ESTABLISHED").contains(snapshot.state())||!(participant.equals(snapshot.caller())||participant.equals(snapshot.winner())))throw new AuthoritySql.FencedException();
        if(participant.equals(snapshot.winner())){var winner=view.participation().winner();if(winner==null||!winner.key().equals(participant.key())||!winner.incarnation().equals(participant.incarnation())||winner.generation()>participant.generation())throw new AuthoritySql.FencedException();}
        var transition=new CallWorkflowService.Transition(snapshot.callId(),group,request.directoryEpoch(),snapshot.version(),command.requestId().value(),CallWorkflowService.Step.READY,snapshot.winner(),List.of(),snapshot.activationId(),view.checkedAt().plusSeconds(5),null,"PENDING_NATIVE_SIGNATURE",null);
        String sealed=home(request,view,transition,"ACTIVE",participant);var base=proofs.decode(sealed,view.sourceCell(),clock.instant()).orElseThrow(AuthoritySql.FencedException::new);var current=route(view,participant);if(current==null)throw new AuthoritySql.FencedException();
        return proofs.issue(new HomeAuthorizationProof.Claims(1,"ACTIVE",base.sourceCell(),base.destinationCell(),command.requestId().value(),base.call(),base.user(),base.session(),base.incarnation(),base.generation(),base.directoryEpoch(),base.storageEpoch(),base.hashVersion(),base.group(),base.groupEpoch(),base.leaseSequence(),base.ownerIncarnation(),base.reservationId(),base.reservationVersion(),base.activationId(),snapshot.version(),snapshot.negotiationId(),command.intentHash(),base.issuedAt(),base.expiresAt(),base.sourceStorageEpoch(),base.participantUntil(),current.connectionId()));
    }
    private static SessionRepository.Route route(HomeProofReadService.View view,Participant participant){return view.currentRoutes().stream().filter(r->r.user().equals(participant.user())&&r.key().equals(participant.key())&&r.incarnation().equals(participant.incarnation())&&r.connectionGeneration()==participant.generation()).findFirst().orElse(null);}
    private static Instant min(Instant a,Instant b){return a.isBefore(b)?a:b;}
}
