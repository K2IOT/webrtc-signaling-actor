package io.webrtc.signaling.rpc;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
/** Coordinator transitions and home effects are separate committed operations; never a network call inside SQL. */
public final class CrossCellSaga {
    public enum Phase {RESERVE_HOME,RING_COORDINATOR,CLAIM_HOME,ACCEPT_COORDINATOR,ACTIVATE_COORDINATOR,CONFIRM_CALLER,CONFIRM_CALLEE,READY_COORDINATOR,RELEASE_HOME}
    @FunctionalInterface public interface Effects {CompletionStage<String> apply(Phase phase,UUID originalOperation,Duration remaining);}
    public record Outcome(UUID operation,String code,boolean reconcileHomeWinner) {}
    private final Effects effects;
    public CrossCellSaga(Effects effects){this.effects=Objects.requireNonNull(effects);}
    public CompletionStage<Outcome> invite(UUID operation,Duration budget){return run(operation,budget,List.of(Phase.RESERVE_HOME,Phase.RING_COORDINATOR),false);}
    public CompletionStage<Outcome> accept(UUID operation,Duration budget){return run(operation,budget,List.of(Phase.CLAIM_HOME,Phase.ACCEPT_COORDINATOR),true);}
    public CompletionStage<Outcome> activate(UUID operation,Duration budget){return run(operation,budget,List.of(Phase.ACTIVATE_COORDINATOR,Phase.CONFIRM_CALLER,Phase.CONFIRM_CALLEE,Phase.READY_COORDINATOR),true);}
    public CompletionStage<Outcome> release(UUID operation,Duration budget){return run(operation,budget,List.of(Phase.RELEASE_HOME),false);}
    private CompletionStage<Outcome> run(UUID operation,Duration budget,List<Phase> phases,boolean winner){Objects.requireNonNull(operation);if(budget==null||budget.isNegative()||budget.isZero())return CompletableFuture.completedFuture(new Outcome(operation,"OUTCOME_UNKNOWN",winner));long end=System.nanoTime()+Math.min(budget.toNanos(),Duration.ofSeconds(2).toNanos());return step(operation,end,phases,0,winner).handle((code,error)->new Outcome(operation,error==null?code:"OUTCOME_UNKNOWN",winner&&(error!=null||"OUTCOME_UNKNOWN".equals(code)||"UNKNOWN".equals(code))));}
    private CompletionStage<String> step(UUID operation,long end,List<Phase> phases,int index,boolean winner){long remaining=end-System.nanoTime();if(remaining<=0)return CompletableFuture.failedFuture(new TimeoutException());CompletionStage<String> stage;try{stage=effects.apply(phases.get(index),operation,Duration.ofNanos(remaining));}catch(RuntimeException failure){return CompletableFuture.failedFuture(failure);}return stage.toCompletableFuture().orTimeout(remaining,TimeUnit.NANOSECONDS).thenCompose(code->{if(Set.of("OUTCOME_UNKNOWN","UNKNOWN","OVERLOADED","UNAVAILABLE","UNAUTHORIZED","INVALID","TERMINAL","ANSWERED_ELSEWHERE","USER_BUSY","FENCED","RELEASED","EXPIRED").contains(code)||index==phases.size()-1)return CompletableFuture.completedFuture(code);return step(operation,end,phases,index+1,winner);});}
}
