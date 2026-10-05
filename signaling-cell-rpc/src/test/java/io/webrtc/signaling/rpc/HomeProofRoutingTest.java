package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.security.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class HomeProofRoutingTest {
    final Instant now=Instant.parse("2026-10-05T00:00:00Z");
    final UUID boot=UUID.randomUUID(),connection=UUID.randomUUID();
    HomeAuthorizationProof.Claims claims(int schema,String purpose,String gateway,UUID bootId){return new HomeAuthorizationProof.Claims(schema,purpose,"c002","c001",UUID.randomUUID(),new CallId("c001.e175.00000000-0000-0000-0000-000000000001"),new UserId("TEST_ONLY_USER"),new SessionKey("TEST_ONLY","callee"),new SessionIncarnation(UUID.randomUUID()),1,1,1,1,175,2,1,UUID.randomUUID(),UUID.randomUUID(),1,UUID.randomUUID(),4,1,"a".repeat(64),now,now.plusSeconds(3),1,now.plusSeconds(30),connection,null,gateway,bootId);}
    @Test void signedActiveV2RetainsExactGatewayBootAndCannotBeRebound()throws Exception {
        var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var signer=new HomeAuthorizationProof("c002","test",key.getPrivate(),Map.of("c002/test",key.getPublic()));
        var original=claims(2,"ACTIVE","TEST_ONLY_GATEWAY",boot);var signed=signer.issue(original);
        assertThat(signer.decode(signed,"c002",now)).contains(original);
        assertThat(signer.decode(signed,"c002",now).orElseThrow().gatewayId()).isEqualTo("TEST_ONLY_GATEWAY");
        assertThat(signer.decode(signed,"c002",now).orElseThrow().bootId()).isEqualTo(boot);
        var parts=signed.split("\\.");var payload=new String(Base64.getUrlDecoder().decode(parts[2]),java.nio.charset.StandardCharsets.UTF_8).replace(boot.toString(),UUID.randomUUID().toString());
        parts[2]=Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(signer.decode(String.join(".",parts),"c002",now)).isEmpty();
    }
    @Test void routeIsMandatoryOnlyForV2ActiveAndCannotLeakIntoLegacyPurposes(){
        for(String purpose:List.of("SESSION","WINNER","COORDINATOR_GRANT","TARGET_ROUTE"))assertThatThrownBy(()->claims(2,purpose,"TEST_ONLY_GATEWAY",boot)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->claims(1,"ACTIVE","TEST_ONLY_GATEWAY",boot)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->claims(2,"ACTIVE",null,boot)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->claims(2,"ACTIVE","TEST_ONLY_GATEWAY",null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->claims(2,"ACTIVE","bad/gateway",boot)).isInstanceOf(IllegalArgumentException.class);
        assertThat(claims(1,"ACTIVE",null,null).schema()).isEqualTo(1);
    }
}
