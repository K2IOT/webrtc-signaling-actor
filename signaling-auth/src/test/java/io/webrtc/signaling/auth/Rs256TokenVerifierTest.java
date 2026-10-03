package io.webrtc.signaling.auth;
import static org.assertj.core.api.Assertions.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class Rs256TokenVerifierTest {
    static KeyPair pair, other;
    static final Instant NOW=Instant.parse("2026-10-03T00:00:00Z");
    static final IdentitySecurityContract CONTRACT=new IdentitySecurityContract("https://issuer.test", "signal-test",
        Duration.ofMinutes(15),Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"test://ordered-security-log");
    @BeforeAll static void keys() throws Exception { var g=KeyPairGenerator.getInstance("RSA");g.initialize(2048);pair=g.generateKeyPair();other=g.generateKeyPair(); }
    static String token(KeyPair key,String header,String claims) throws Exception {
        var enc=Base64.getUrlEncoder().withoutPadding();
        String input=enc.encodeToString(header.getBytes(java.nio.charset.StandardCharsets.UTF_8))+"."+enc.encodeToString(claims.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var s=Signature.getInstance("SHA256withRSA");s.initSign(key.getPrivate());s.update(input.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        return input+"."+enc.encodeToString(s.sign());
    }
    static String claims() { return "{\"userId\":\"alice\",\"jti\":\"j-a\",\"iss\":\"https://issuer.test\",\"aud\":[\"signal-test\"],\"iat\":"+NOW.getEpochSecond()+",\"exp\":"+NOW.plusSeconds(900).getEpochSecond()+"}"; }
    static String header() {return "{\"alg\":\"RS256\",\"kid\":\"key-a\"}";}
    Rs256TokenVerifier verifier() {return new Rs256TokenVerifier(CONTRACT,new TrustedRsaKeys(Map.of("key-a",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);}
    @Test void validatesRealRs256WithoutNetwork() throws Exception {var p=verifier().validate(token(pair,header(),claims()),NOW);assertThat(p.userId().value()).isEqualTo("alice");assertThat(p.key().jti()).isEqualTo("j-a");}
    @Test void issuerUsesItsOwn512ByteBoundRatherThanJti256ByteBound()throws Exception {
        String issuer="https://"+"a".repeat(504);var contract=new IdentitySecurityContract(issuer,CONTRACT.audience(),CONTRACT.maximumJwtLifetime(),CONTRACT.clockSkew(),CONTRACT.revocationPropagationSLO(),CONTRACT.hardSafetyBound(),true,CONTRACT.highWaterSource());
        var verifier=new Rs256TokenVerifier(contract,new TrustedRsaKeys(Map.of("key-a",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);assertThat(verifier.validate(token(pair,header(),claims().replace(CONTRACT.issuer(),issuer)),NOW).key().issuer()).isEqualTo(issuer);
    }
    @Test void rejectsWrongKeyAndAlteredPayloadOrSignature() throws Exception {
        var jwt=token(pair,header(),claims());
        assertThatThrownBy(()->verifier().validate(token(other,header(),claims()),NOW)).isInstanceOf(AuthException.class);
        assertThatThrownBy(()->verifier().validate(jwt.substring(0,jwt.length()-4)+"AAAA",NOW)).isInstanceOf(AuthException.class);
        assertThatThrownBy(()->verifier().validate(token(pair,header(),claims()).replace(jwt.split("\\.")[1],Base64.getUrlEncoder().withoutPadding().encodeToString(claims().replace("alice","bob").getBytes())),NOW)).isInstanceOf(AuthException.class);
    }
    @ParameterizedTest @ValueSource(strings={"none","HS256","PS256","ES256"}) void rejectsOtherAlgorithms(String alg) throws Exception {assertThatThrownBy(()->verifier().validate(token(pair,header().replace("RS256",alg),claims()),NOW)).isInstanceOf(AuthException.class);}
    @Test void validatesTimeIssuerAudienceAndRequiredClaims() throws Exception {
        for(String c:List.of(claims().replace("https://issuer.test","evil"),claims().replace("signal-test","wrong"),claims().replace("\"userId\":\"alice\",",""),claims().replace("\"jti\":\"j-a\",",""),claims().replace("\"iat\":"+NOW.getEpochSecond()+",",""),claims().replace("\"exp\":"+NOW.plusSeconds(900).getEpochSecond(),"\"exp\":"+NOW.minusSeconds(31).getEpochSecond()),claims().replace("\"exp\":","\"nbf\":"+NOW.plusSeconds(31).getEpochSecond()+",\"exp\":"),claims().replace("\"exp\":"+NOW.plusSeconds(900).getEpochSecond(),"\"exp\":"+NOW.plusSeconds(901).getEpochSecond())))
            assertThatThrownBy(()->verifier().validate(token(pair,header(),c),NOW)).isInstanceOf(AuthException.class);
    }
    @Test void refusesUndersizedKeysAndUsesRotationOverlapAndRetirement() throws Exception {
        var g=KeyPairGenerator.getInstance("RSA");g.initialize(1024);var small=g.generateKeyPair();
        assertThatThrownBy(()->new TrustedRsaKeys(Map.of("small",(RSAPublicKey)small.getPublic()),null,Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        var keys=new TrustedRsaKeys(Map.of("key-a",(RSAPublicKey)pair.getPublic(),"key-b",(RSAPublicKey)other.getPublic()),null,Duration.ofSeconds(1));
        var v=new Rs256TokenVerifier(CONTRACT,keys,8192);
        v.validate(token(pair,header(),claims()),NOW);v.validate(token(other,header().replace("key-a","key-b"),claims()),NOW);
        keys.retire("key-a");assertThatThrownBy(()->v.validate(token(pair,header(),claims()),NOW)).isInstanceOf(AuthException.class);
    }
    @Test void unknownKidRefreshIsSingleFlightAndRateLimited() {
        var count=new AtomicInteger();var pending=new CompletableFuture<Map<String,RSAPublicKey>>();
        var keys=new TrustedRsaKeys(Map.of("key-a",(RSAPublicKey)pair.getPublic()),()->{count.incrementAndGet();return pending;},Duration.ofSeconds(1));
        for(int i=0;i<100;i++)assertThat(keys.lookup("unknown",NOW)).isEmpty();assertThat(count.get()).isEqualTo(1);
        pending.complete(Map.of("key-a",(RSAPublicKey)pair.getPublic(),"unknown",(RSAPublicKey)other.getPublic()));
        assertThat(keys.lookup("unknown",NOW).orElseThrow()).isEqualTo(other.getPublic());
    }
}
