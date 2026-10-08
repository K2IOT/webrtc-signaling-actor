package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.net.URI;
import java.security.KeyPairGenerator;
import java.time.*;
import java.util.*;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;

class NativeCallingPolicyTest {
    private ApplicationContextRunner main(String profile)throws Exception {
        var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
        return new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
            .withInitializer(context->{defaults.forEach(source->context.getEnvironment().getPropertySources().addLast(source));context.getEnvironment().setActiveProfiles(profile);})
            .withPropertyValues("signaling.identity.issuer=TEST_ONLY_ISSUER","signaling.identity.audience=TEST_ONLY_AUDIENCE");
    }
    private NativeActorSourceEnrollment enrollment(Duration hard)throws Exception {
        var identity=new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofMinutes(10),Duration.ofSeconds(30),hard.dividedBy(2),hard,true,"TEST_ONLY_SOURCE");
        var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
        var endpoint=new NativeActorSourceEnrollment.Endpoint(URI.create("https://localhost:1/v1/source"),SSLContext.getInstance("TLSv1.3"),Map.of("TEST_ONLY_SOURCE",key));
        return new NativeActorSourceEnrollment("c001",1,UUID.randomUUID(),UUID.randomUUID(),identity,endpoint,endpoint);
    }
    private AuthPrincipal caller(Instant expiry) {
        return new AuthPrincipal(new UserId("alice"),new SessionKey("TEST_ONLY_ISSUER","TEST_ONLY_JTI"),expiry,Instant.EPOCH,"TEST_ONLY_KEY",1);
    }
    @Test void mainSelectsOpenAuthenticatedPolicyWithVersionAndEnrolledFreshness()throws Exception {
        var now=Instant.parse("2026-10-08T00:00:00Z");
        for(var hard:List.of(Duration.ofSeconds(1),Duration.ofSeconds(8))){
            var source=enrollment(hard);var age=hard.compareTo(Duration.ofSeconds(5))<0?hard:Duration.ofSeconds(5);
            main("actor").withBean(NativeActorSourceEnrollment.class,()->source).run(context->{
                assertThat(context).hasNotFailed().hasSingleBean(CallAuthorizationPolicy.class);
                var policy=context.getBean(CallAuthorizationPolicy.class);
                var allowed=policy.authorize(new CallAuthorizationRequest(caller(now.plusSeconds(60)),new UserId("bob"),now)).toCompletableFuture().join();
                assertThat(allowed.allowed()).isTrue();assertThat(allowed.policyVersion()).isEqualTo("open-authenticated-v1");
                assertThat(allowed.expiresAt()).isEqualTo(now.plus(age));
                var expires=now.plusMillis(100);
                assertThat(policy.authorize(new CallAuthorizationRequest(caller(expires),new UserId("bob"),now)).toCompletableFuture().join().expiresAt()).isEqualTo(expires);
                assertThat(policy.authorize(new CallAuthorizationRequest(caller(now),new UserId("bob"),now)).toCompletableFuture().join().allowed()).isFalse();
                assertThat(policy.authorize(new CallAuthorizationRequest(caller(now.plusSeconds(60)),new UserId("alice"),now)).toCompletableFuture().join().allowed()).isFalse();
            });
        }
    }
    @Test void missingEnrollmentAndOtherPlanesCannotManufactureAnOpenPolicy()throws Exception {
        main("actor").run(context->assertThat(context).hasNotFailed().doesNotHaveBean(CallAuthorizationPolicy.class));
        var source=enrollment(Duration.ofSeconds(1));
        for(String plane:List.of("gateway","control"))main(plane).withBean(NativeActorSourceEnrollment.class,()->source)
            .run(context->assertThat(context).hasNotFailed().doesNotHaveBean(CallAuthorizationPolicy.class));
    }
    @Test void explicitPolicyOwnerRemainsAuthoritative()throws Exception {
        var source=enrollment(Duration.ofSeconds(1));var denied=CallAuthorizationPolicy.denyAll();
        main("actor").withBean(NativeActorSourceEnrollment.class,()->source).withBean(CallAuthorizationPolicy.class,()->denied)
            .run(context->assertThat(context).hasNotFailed().hasSingleBean(CallAuthorizationPolicy.class)
                .getBean(CallAuthorizationPolicy.class).isSameAs(denied));
    }
}
