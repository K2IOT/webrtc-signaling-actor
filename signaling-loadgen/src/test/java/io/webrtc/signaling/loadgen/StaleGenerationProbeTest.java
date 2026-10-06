package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StaleGenerationProbeTest {
    private final ObjectMapper json=new ObjectMapper();
    private JsonNode auth(UUID incarnation,String generation){return json.createObjectNode().put("v",1).put("type","AUTH_OK").put("sessionIncarnation",incarnation.toString()).put("connectionGeneration",generation);}
    @Test void replacementRequiresSameNativeIncarnationAndStrictlyNewerNativeGeneration(){
        var id=UUID.randomUUID();var original=auth(id,"2");
        assertThat(StaleGenerationProbe.replacement(original,auth(id,"3"))).isTrue();
        for(var invalid:new JsonNode[]{auth(UUID.randomUUID(),"3"),auth(id,"2"),auth(id,"1"),auth(id,"0"),auth(id,"03"),auth(id,"9223372036854775808"),json.createObjectNode()})
            assertThat(StaleGenerationProbe.replacement(original,invalid)).as(invalid.toString()).isFalse();
        assertThat(StaleGenerationProbe.replacement(auth(id,"0"),auth(id,"3"))).isFalse();
    }
    @Test void closeReasonAloneCannotProveNativeReplacement(){
        var id=UUID.randomUUID();var original=auth(id,"2");
        var close=new VirtualClient.ProbeReceipt(VirtualClient.ProbeKind.SECURITY_CLOSURE,1,1,2,VirtualClient.ProbeOutcome.AUTHORIZATION_REJECTED,1008,VirtualClient.CloseReason.STALE_CONNECTION);
        assertThat(StaleGenerationProbe.classify(original,auth(id,"3"),close)).isEqualTo(StaleGenerationProbe.Outcome.REPLACED);
        assertThat(StaleGenerationProbe.classify(original,auth(id,"2"),close)).isEqualTo(StaleGenerationProbe.Outcome.BINDING_MISMATCH);
        assertThat(StaleGenerationProbe.classify(original,null,close)).isEqualTo(StaleGenerationProbe.Outcome.REPLACEMENT_UNKNOWN);
        var revoked=new VirtualClient.ProbeReceipt(VirtualClient.ProbeKind.SECURITY_CLOSURE,1,1,2,VirtualClient.ProbeOutcome.AUTHORIZATION_REJECTED,1008,VirtualClient.CloseReason.AUTH_REVOKED);
        assertThat(StaleGenerationProbe.classify(original,auth(id,"3"),revoked)).isEqualTo(StaleGenerationProbe.Outcome.CLOSURE_UNKNOWN);
    }
}
