package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;

class LoadGeneratorContractTest {
    @TempDir Path scratch;
    @Test void distributedRangesCoverTenMillionOnceAcrossUnevenWorkerCounts(){
        for(int workers:new int[]{1,7,53,4096}){
            long end=0;for(int worker=0;worker<workers;worker++){var range=DistributedLoadGenerator.partition(10_000_000,worker,workers);assertThat(range.start()).isEqualTo(end);assertThat(range.end()).isGreaterThanOrEqualTo(range.start());end=range.end();}assertThat(end).isEqualTo(10_000_000);
        }
        assertThatThrownBy(()->DistributedLoadGenerator.partition(10,2,2)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void identifiersAreReproducibleAndDoNotAliasWorkersOrRetries(){
        assertThat(DistributedLoadGenerator.operation(42,99,"INVITE",1)).isEqualTo(DistributedLoadGenerator.operation(42,99,"INVITE",1));
        assertThat(DistributedLoadGenerator.operation(42,99,"INVITE",1)).isNotEqualTo(DistributedLoadGenerator.operation(42,100,"INVITE",1)).isNotEqualTo(DistributedLoadGenerator.operation(42,99,"INVITE",2));
    }
    @Test void arrivalClockIncludesDispatchStallRatherThanResettingAtSend(){
        assertThat(ScenarioRunner.arrivalNanos(1_000,10_001,10_000)).isEqualTo(1_000_101_000L);
        var evidence=new EvidenceWriter();evidence.record(1_000,1_000_101_000L,false);
        assertThat(evidence.attempts()).isEqualTo(1);assertThat(evidence.successes()).isZero();assertThat(evidence.percentileMillis(99.9)).isGreaterThanOrEqualTo(1000);
    }
    @Test void sixtySecondDoubleBurstReturnsToBaselineWithoutSixtySecondArrivalGap(){
        assertThat(ScenarioRunner.burstArrivalNanos(0,1_200_000,10_000,2,60)).isEqualTo(60_000_000_000L);
        assertThat(ScenarioRunner.burstArrivalNanos(0,1_200_001,10_000,2,60)).isEqualTo(60_000_100_000L);
        assertThat(ScenarioRunner.burstArrivalNanos(0,1_199_999,10_000,2,60)).isEqualTo(59_999_950_000L);
    }
    @Test void lateCloseOfOldSocketCannotSubtractAReauthenticatedConnection(){
        var gauge=new ScenarioRunner.SocketGauge();gauge.authenticated(1,1);gauge.authenticated(1,1);assertThat(gauge.live()).isEqualTo(1);gauge.closed(1,1);assertThat(gauge.live()).isZero();gauge.authenticated(1,2);gauge.closed(1,1);assertThat(gauge.live()).isEqualTo(1);assertThat(gauge.peak()).isEqualTo(1);gauge.closed(1,2);assertThat(gauge.live()).isZero();
    }
    @Test void syncUsesTypedNativeRoundIdsWithoutSearchingSerializedDeadlineMetadata()throws Exception {
        var json=new com.fasterxml.jackson.databind.ObjectMapper();
        var reply=json.readTree("{\"negotiationId\":\"7\",\"iceGeneration\":\"9\",\"result\":{\"deadlines\":{\"negotiationUntil\":\"2026-10-04T00:00:20Z\"}}}");
        var ids=ScenarioRunner.roundIds(reply).orElseThrow();assertThat(ids.negotiation()).isEqualTo("7");assertThat(ids.ice()).isEqualTo("9");
        assertThat(ScenarioRunner.roundIds(json.readTree("{\"negotiationId\":\"0\",\"iceGeneration\":\"0\"}"))).isEmpty();
        assertThatThrownBy(()->ScenarioRunner.roundIds(json.readTree("{\"negotiationId\":\"1\",\"iceGeneration\":\"0\"}"))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void creditBoundCountsBytesAndRetainsExactlyOneCompletionOwner(){
        var credits=new VirtualClient.Credits(2,100);var first=credits.acquire(70);assertThat(first).isNotNull();assertThat(credits.acquire(31)).isNull();var second=credits.acquire(30);assertThat(second).isNotNull();assertThat(credits.acquire(1)).isNull();first.close();first.close();assertThat(credits.count()).isEqualTo(1);assertThat(credits.bytes()).isEqualTo(30);second.close();assertThat(credits.bytes()).isZero();
    }
    @Test void histogramExportContainsActualSamplesAndCanBeDecoded()throws Exception {
        var evidence=new EvidenceWriter();evidence.record(0,2_000_000,true);evidence.record(0,20_000_000,false);var file=scratch.resolve("latency.hdr");evidence.export(file);assertThat(EvidenceWriter.read(file).getTotalCount()).isEqualTo(2);
    }
    @Test void checkedPartitionCannotOverflowOrProduceNegativeGlobalIndices(){
        assertThatThrownBy(()->DistributedLoadGenerator.partition(-1,0,1)).isInstanceOf(IllegalArgumentException.class);
        var last=DistributedLoadGenerator.partition(Long.MAX_VALUE,52,53);assertThat(last.end()).isEqualTo(Long.MAX_VALUE);assertThat(last.start()).isPositive();
    }
}
