package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class CallScheduleTest {
    @Test void workerCallSchedulesCoverPrimaryCallerAndCalleeUsersOnceWithSeparateRoles(){
        var callers=new HashSet<Long>();var targets=new HashSet<Long>();var ordinals=new HashSet<Long>();
        for(int worker=0;worker<7;worker++){
            var range=DistributedLoadGenerator.partition(1000,worker,7);var schedule=new CallSchedule(20261004,800,range);
            while(true){var next=schedule.nextDue(n->n*1_000_000,399_000_000);if(next.isEmpty())break;var call=next.get();assertThat(call.callerSocket()).isBetween(range.start(),range.end()-1);assertThat(call.callerSocket()).isLessThan(400);assertThat(call.targetUser()).isBetween(400L,799L);assertThat(callers.add(call.callerSocket())).isTrue();assertThat(targets.add(call.targetUser())).isTrue();assertThat(ordinals.add(call.ordinal())).isTrue();}
        }
        assertThat(callers).hasSize(400);assertThat(targets).hasSize(400);assertThat(ordinals).hasSize(400);
    }
    @Test void arrivalsUseTheOriginalGlobalClockAndRoundWithoutRandomBusySelection(){
        var range=DistributedLoadGenerator.partition(10,0,1);var a=new CallSchedule(42,8,range);var b=new CallSchedule(42,8,range);
        for(int i=0;i<8;i++){var first=a.nextDue(n->n*100,700).orElseThrow();assertThat(first).isEqualTo(b.nextDue(n->n*100,700).orElseThrow());assertThat(first.ordinal()).isEqualTo(i);assertThat(first.intendedNanos()).isEqualTo(i*100L);}
        assertThat(a.nextDue(n->n*100,700)).isEmpty();
    }
    @Test void secondarySessionsJoinCalleeUsersForActualWinnerFanout(){
        for(int index=0;index<8;index++)assertThat(DistributedLoadGenerator.userIndex(index,8)).isEqualTo(index);
        assertThat(DistributedLoadGenerator.userIndex(8,8)).isEqualTo(4);assertThat(DistributedLoadGenerator.userIndex(9,8)).isEqualTo(5);
        var evidence=new EvidenceWriter();evidence.missed(EvidenceWriter.Operation.INVITE,0,1_000_000);
        var phases=(Map<?,?>)evidence.snapshot().get("latencies");assertThat(((Map<?,?>)phases.get("INVITE")).get("samples")).isEqualTo(1L);assertThat(evidence.successes()).isZero();
    }
    @Test void secondarySessionsOwnNoAdditionalOutgoingCallStream(){
        var schedule=new CallSchedule(42,8,new DistributedLoadGenerator.Range(8,10));
        assertThat(schedule.nextDue(n->n,Long.MAX_VALUE)).isEmpty();
    }
}
