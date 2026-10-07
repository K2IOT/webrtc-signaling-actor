package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.control.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** Native HTTPS/RSA/PostgreSQL Main; signed clock and cached security adapters are TEST_ONLY. */
class NativeControlMainIT {
    @Test void nativeMainServesTrustedHttpsBootstrapAndRejectsLossOfClockOrSecurity()throws Exception {
        var rsa=KeyPairGenerator.getInstance("RSA");rsa.initialize(2048);var keys=rsa.generateKeyPair();
        var now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        var security=new AtomicReference<>(AuthorizationStatus.ALLOWED);var fresh=new java.util.concurrent.atomic.AtomicBoolean(true);
        try(var database=new LocalInviteAtomicIT.Fixture();var clockFixture=new NativeGatewayCommandIT.MainGateway(KeyPairGenerator.getInstance("Ed25519").generateKeyPair())){
            Flyway.configure().dataSource(database.url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword()).table("TEST_ONLY_directory_history").baselineOnMigrate(true).baselineVersion("0").locations("classpath:db/directory/migration").load().migrate();
            try(var c=database.connection();var q=c.createStatement()){q.execute("INSERT INTO directory_bucket VALUES(175,'c001',7,'wss://cell.test/ws',clock_timestamp())");}
            var directory=new PostgresDirectoryRepository(database.runtime.sql,database.runtime.sql,"c001",1,()->true);
            var contract=new IdentitySecurityContract("https://issuer.test","control-test",Duration.ofMinutes(15),Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"TEST_ONLY_HIGH_WATER");
            try(var tokens=new BoundedTokenVerifier(new Rs256TokenVerifier(contract,new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keys.getPublic()),null,Duration.ofSeconds(1)),8192),1,8,Duration.ofSeconds(1))){
                clockFixture.refreshClock();clockFixture.reports.scheduleAtFixedRate(clockFixture::refreshClock,1,1,TimeUnit.SECONDS);
                var business=new NativeControlBusinessEnrollment(directory,tokens,(p,n)->security.get(),fresh::get,clockFixture.clock,Duration.ofSeconds(30));
                var tls=io.netty.handler.ssl.SslContextBuilder.forServer(NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key")).sslProvider(io.netty.handler.ssl.SslProvider.JDK).protocols("TLSv1.3").build();
                var inputs=new NativeControlIngressEnrollment(new InetSocketAddress("127.0.0.1",0),tls,new InetSocketAddress("127.0.0.1",0),()->true,()->"TEST_ONLY 1\n");
                var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
                var application=SignalingApplication.application();application.setWebApplicationType(WebApplicationType.REACTIVE);application.setRegisterShutdownHook(false);
                application.addInitializers(context->{defaults.forEach(s->context.getEnvironment().getPropertySources().addLast(s));context.getBeanFactory().registerSingleton("TEST_ONLY_business",business);context.getBeanFactory().registerSingleton("TEST_ONLY_ingress",inputs);});
                try(var context=application.run("--spring.profiles.active=control","--spring.main.web-application-type=reactive","--signaling.identity.issuer=https://issuer.test","--signaling.identity.audience=control-test")){
                    assertThat(context.getBeansOfType(BootstrapController.class)).hasSize(1);
                    int port=((WebServerApplicationContext)context).getWebServer().getPort();
                    var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);
                    try(var in=new java.io.FileInputStream(NativeGatewayCommandIT.cert("ca.crt"))){store.setCertificateEntry("TEST_ONLY",java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));}
                    var trust=javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());trust.init(store);var clientTls=javax.net.ssl.SSLContext.getInstance("TLSv1.3");clientTls.init(null,trust.getTrustManagers(),null);
                    try(var client=HttpClient.newBuilder().sslContext(clientTls).connectTimeout(Duration.ofSeconds(2)).build()){
                        var request=HttpRequest.newBuilder(URI.create("https://localhost:"+port+"/v1/signaling/bootstrap")).timeout(Duration.ofSeconds(3)).header("Authorization","Bearer "+token(keys,now)).POST(HttpRequest.BodyPublishers.noBody()).build();
                        var response=client.send(request,HttpResponse.BodyHandlers.ofString());
                        assertThat(response.statusCode()).isEqualTo(200);
                        var body=NativeGatewayCommandIT.JSON.readTree(response.body());assertThat(body.path("directoryEpoch").isTextual()).isTrue();assertThat(body.path("directoryEpoch").asLong()).isEqualTo(7);assertThat(body.path("wssUrl").asText()).isEqualTo("wss://cell.test/ws");
                        var health=context.getBean(PrivateHealthServer.class);int healthPort=health.port();
                        assertThat(client.send(PrivateHealthServerTestRequest.of(healthPort,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
                        fresh.set(false);assertThat(client.send(request,HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
                        assertThat(client.send(PrivateHealthServerTestRequest.of(healthPort,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);fresh.set(true);
                        security.set(AuthorizationStatus.REVOKED);assertThat(client.send(request,HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
                        security.set(AuthorizationStatus.ALLOWED);clockFixture.reports.shutdownNow();clockFixture.clock.invalidate();
                        assertThat(client.send(request,HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
                        assertThat(client.send(PrivateHealthServerTestRequest.of(healthPort,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
                        clockFixture.refreshClock();
                        assertThat(client.send(PrivateHealthServerTestRequest.of(healthPort,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
                        context.stop();
                        assertThat(client.send(PrivateHealthServerTestRequest.of(healthPort,"/ready"),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(503);
                        context.close();assertThatThrownBy(()->new Socket("127.0.0.1",port)).isInstanceOf(java.io.IOException.class);assertThatThrownBy(()->new Socket("127.0.0.1",healthPort)).isInstanceOf(java.io.IOException.class);
                    }
                }
            }
        }
    }
    static String token(KeyPair keys,Instant now)throws Exception {
        var encoding=Base64.getUrlEncoder().withoutPadding();
        var header="{\"alg\":\"RS256\",\"kid\":\"test\"}";
        var claims="{\"iss\":\"https://issuer.test\",\"aud\":\"control-test\",\"userId\":\"alice\",\"jti\":\"TEST_ONLY\",\"iat\":"+now.getEpochSecond()+",\"exp\":"+now.plusSeconds(600).getEpochSecond()+"}";
        var input=encoding.encodeToString(header.getBytes(StandardCharsets.UTF_8))+"."+encoding.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        var signature=Signature.getInstance("SHA256withRSA");signature.initSign(keys.getPrivate());signature.update(input.getBytes(StandardCharsets.US_ASCII));return input+"."+encoding.encodeToString(signature.sign());
    }
    static final class PrivateHealthServerTestRequest {
        static HttpRequest of(int port,String path){return HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(2)).GET().build();}
    }
}
