package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Optional;

/** A snapshot grants fresh work; it does not attest retention of an earlier volatile SDP. */
final class SnapshotOffer {
    record Session(String user,String issuer,String jti,String incarnation,String generation) {
        @Override public String toString(){return "Session[redacted]";}
        boolean matches(JsonNode participant){return user.equals(participant.path("userId").asText())&&issuer.equals(participant.path("issuer").asText())&&jti.equals(participant.path("jti").asText())&&incarnation.equals(participant.path("sessionIncarnation").asText())&&generation.equals(participant.path("connectionGeneration").asText());}
    }
    static Optional<ScenarioRunner.RoundIds> freshRound(JsonNode snapshot,Session local,long lastOfferedRound) {
        var result=snapshot.path("result");var negotiation=result.path("negotiation");
        if(local==null||!snapshot.path("type").asText().equals("CALL_SNAPSHOT")||!result.path("code").asText().equals("SNAPSHOT")
            ||!List.of("CONNECTING","ESTABLISHED").contains(result.path("state").asText())||!negotiation.path("state").asText().equals("OFFER_GRANTED"))return Optional.empty();
        var ids=ScenarioRunner.roundIds(snapshot);if(ids.isEmpty())throw new IllegalArgumentException("Missing granted native round");
        var round=ids.get();if(!round.negotiation().equals(negotiation.path("negotiationId").asText())||!round.ice().equals(negotiation.path("iceGeneration").asText()))throw new IllegalArgumentException("Inconsistent native round");
        if(Long.parseLong(round.negotiation())<=lastOfferedRound)return Optional.empty();
        var offerer=negotiation.path("offerer");if(!local.matches(offerer))return Optional.empty();
        JsonNode participant=local.matches(result.path("caller"))?result.path("caller"):local.matches(result.path("winner"))?result.path("winner"):null;
        if(participant==null||!participant.path("connectionId").isTextual()||participant.path("connectionId").asText().isEmpty()||!participant.path("connectionId").equals(offerer.path("connectionId")))return Optional.empty();
        return ids;
    }
    static boolean needsFreshRound(JsonNode snapshot,Session local,long lastOfferedRound,boolean recovering) {
        var result=snapshot.path("result");var negotiation=result.path("negotiation");
        if(local==null||!snapshot.path("type").asText().equals("CALL_SNAPSHOT")||!result.path("code").asText().equals("SNAPSHOT")||!List.of("CONNECTING","ESTABLISHED").contains(result.path("state").asText()))return false;
        var participant=local.matches(result.path("caller"))?result.path("caller"):local.matches(result.path("winner"))?result.path("winner"):null;
        if(participant==null||!participant.path("connectionId").isTextual()||participant.path("connectionId").asText().isBlank())return false;
        if(negotiation.path("state").asText().equals("INVALIDATED"))return true;
        if(!recovering)return false;
        if(negotiation.path("state").asText().equals("COMPLETE"))return true;
        var ids=ScenarioRunner.roundIds(snapshot);
        return ids.isPresent()&&Long.parseLong(ids.get().negotiation())<=lastOfferedRound;
    }
    private SnapshotOffer(){}
}
