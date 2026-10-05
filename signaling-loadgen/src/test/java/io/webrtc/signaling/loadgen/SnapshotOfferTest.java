package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SnapshotOfferTest {
    private static final ObjectMapper JSON=new ObjectMapper();
    private final SnapshotOffer.Session local=new SnapshotOffer.Session("user","issuer","jti","incarnation","2");
    private ObjectNode snapshot(String phase) {
        var reply=JSON.createObjectNode().put("type","CALL_SNAPSHOT").put("negotiationId","7").put("iceGeneration","9");
        var result=reply.putObject("result").put("state","CONNECTING").put("code","SNAPSHOT");
        var caller=result.putObject("caller").put("userId","user").put("issuer","issuer").put("jti","jti").put("sessionIncarnation","incarnation").put("connectionGeneration","2").put("connectionId","native-connection");
        var round=result.putObject("negotiation").put("state",phase).put("negotiationId","7").put("iceGeneration","9");round.set("offerer",caller.deepCopy());return reply;
    }
    @Test void completedHealthyNegotiationDoesNotReplayOldSdpOnReconnect() {
        var reply=snapshot("COMPLETE");reply.withObject("result").put("state","ESTABLISHED");
        assertThat(SnapshotOffer.freshRound(reply,local,0)).isEmpty();
    }
    @Test void onlyCurrentSelectedNativeOffererCanStartFreshGrantedRound() {
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),local,0)).hasValue(new ScenarioRunner.RoundIds("7","9"));
        var other=new SnapshotOffer.Session("user","issuer","other-jti","incarnation","2");
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),other,0)).isEmpty();
        var newer=new SnapshotOffer.Session("user","issuer","jti","incarnation","3");
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),newer,0)).isEmpty();
    }
    @Test void repeatedOrOlderSnapshotNeverCreatesAnotherLocalOfferTrace() {
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),local,7)).isEmpty();
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),local,8)).isEmpty();
    }
    @Test void invalidatedOrUnretainedNegotiationDoesNotInventSdpRetention() {
        assertThat(SnapshotOffer.freshRound(snapshot("INVALIDATED"),local,0)).isEmpty();
        assertThat(SnapshotOffer.freshRound(snapshot("OFFER_GRANTED"),null,0)).isEmpty();
    }
    @Test void staleNativeConnectionAndInconsistentRoundCannotMintLocalOffer() {
        var stale=snapshot("OFFER_GRANTED");stale.withObject("result").withObject("negotiation").withObject("offerer").put("connectionId","old-connection");
        assertThat(SnapshotOffer.freshRound(stale,local,0)).isEmpty();
        var invalid=snapshot("OFFER_GRANTED");invalid.put("iceGeneration","10");
        assertThatThrownBy(()->SnapshotOffer.freshRound(invalid,local,0)).isInstanceOf(IllegalArgumentException.class);
    }
}
