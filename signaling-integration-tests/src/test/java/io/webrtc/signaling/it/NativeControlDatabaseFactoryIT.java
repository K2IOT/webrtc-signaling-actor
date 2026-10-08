package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import java.time.Duration;
import java.util.*;
import java.net.URI;
import java.security.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.webrtc.signaling.auth.*;
import org.springframework.beans.factory.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class NativeControlDatabaseFactoryIT {
    @Test void actualMainCreatesOwnedDatabasesAndClockPollingWithoutInferringHealthySource()throws Exception {
        var polls=new AtomicInteger();
        try(var fixture=new NativeControlStartupFailureIT.Fixture();var local=new LocalInviteAtomicIT.Fixture()){
            var app=fixture.application();
            app.addInitializers(context->{
                var beans=(DefaultListableBeanFactory)context.getBeanFactory();beans.destroySingleton("TEST_ONLY_business");beans.destroySingleton("TEST_ONLY_sql");
                beans.registerSingleton("TEST_ONLY_databases",new NativeControlDatabaseEnrollment(database(fixture.database.url),database(local.url),"c001",1,()->false));
                beans.registerSingleton("TEST_ONLY_source",new NativeControlSourceEnrollment("c001",1,fixture.clock.pod,fixture.clock.processBoot,
                    new NativeActorSourceEnrollment.Endpoint(URI.create("https://localhost:1/clock"),clientTls(),Map.of("TEST_ONLY",fixture.clock.signing.getPublic())),event->polls.incrementAndGet()));
                beans.registerBeanDefinition("TEST_ONLY_managedBusiness",new RootBeanDefinition(NativeControlBusinessEnrollment.class,()->new NativeControlBusinessEnrollment(
                    context.getBean(PostgresDirectoryRepository.class),fixture.tokens,(p,n)->AuthorizationStatus.ALLOWED,()->true,context.getBean(ClockSafetyMonitor.class),Duration.ofSeconds(30))));
            });
            NativeControlDatabaseResources databases;
            try(var context=fixture.run(app)){
                databases=context.getBean(NativeControlDatabaseResources.class);
                assertThat(context.getBeansOfType(NativeClockSource.class)).hasSize(1);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->polls.get()>0);
                assertThat(context.getBean(NativeControlReadiness.class).ready()).isFalse();
                assertThat(context.getBean(ClockSafetyMonitor.class).valid()).isFalse();
                var directory=context.getBean(PostgresDirectoryRepository.class);
                assertThat(directory.localActive(175,"c001",1).toCompletableFuture().join()).isTrue();
                assertThatThrownBy(()->directory.read(175).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            }
            assertThat(databases.regional().pools().closed()).isTrue();assertThat(databases.local().pools().closed()).isTrue();
        }
    }
    @Test void actualMainFailureBeforeSqlAliasesStillRetiresPublishedManagedDatabases()throws Exception {
        var created=new AtomicReference<NativeControlDatabaseResources>();
        try(var fixture=new NativeControlStartupFailureIT.Fixture();var local=new LocalInviteAtomicIT.Fixture()){
            var app=fixture.application();app.addInitializers(context->{
                var beans=(DefaultListableBeanFactory)context.getBeanFactory();beans.destroySingleton("TEST_ONLY_business");beans.destroySingleton("TEST_ONLY_sql");
                beans.registerSingleton("TEST_ONLY_databases",new NativeControlDatabaseEnrollment(database(fixture.database.url),database(local.url),"c001",1,()->false));
                beans.registerBeanDefinition("TEST_ONLY_failedBusiness",new RootBeanDefinition(NativeControlBusinessEnrollment.class,()->{
                    created.set(context.getBean(NativeControlDatabaseResources.class));
                    throw new IllegalStateException("TEST_ONLY fail before SQL aliases");
                }));
            });
            assertThat(catchThrowable(()->fixture.run(app))).hasRootCauseMessage("TEST_ONLY fail before SQL aliases");
            assertThat(created.get()).isNotNull();assertThat(created.get().regional().pools().closed()).isTrue();assertThat(created.get().local().pools().closed()).isTrue();
        }
    }
    static javax.net.ssl.SSLContext clientTls(){
        try{
            var trust=KeyStore.getInstance(KeyStore.getDefaultType());trust.load(null,null);
            try(var input=new java.io.FileInputStream(NativeGatewayCommandIT.cert("ca.crt"))){trust.setCertificateEntry("TEST_ONLY",java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(input));}
            var factory=javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());factory.init(trust);
            var tls=javax.net.ssl.SSLContext.getInstance("TLSv1.3");tls.init(null,factory.getTrustManagers(),null);return tls;
        }catch(Exception failed){throw new IllegalStateException("TEST_ONLY source trust",failed);}
    }
    @Test void mainFactoryCreatesSeparateFixedNativeSqlOwnersAndFailureRetiresThem()throws Exception {
        try(var regional=new LocalInviteAtomicIT.Fixture();var local=new LocalInviteAtomicIT.Fixture()){
            var enrollment=new NativeControlDatabaseEnrollment(database(regional.url),database(local.url),"c001",1,()->false);
            var captured=new ArrayList<SqlTransactions>();
            var defaults=new org.springframework.boot.env.YamlPropertySourceLoader().load("TEST_ONLY_defaults",new org.springframework.core.io.FileSystemResource("../config/production-defaults.yaml"));
            new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
                .withInitializer(context->{defaults.forEach(source->context.getEnvironment().getPropertySources().addLast(source));context.getEnvironment().setActiveProfiles("control");})
                .withBean(NativeControlDatabaseEnrollment.class,()->enrollment)
                .withPropertyValues("signaling.identity.issuer=TEST_ONLY","signaling.identity.audience=TEST_ONLY")
                .run(context->{
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBeansOfType(SqlTransactions.class)).hasSize(2);
                    var directory=context.getBean(PostgresDirectoryRepository.class);
                    captured.addAll(directory.transactionOwners());assertThat(captured).hasSize(2);
                    assertThat(captured.get(0).pools()).isNotSameAs(captured.get(1).pools());
                    assertThat(captured.get(0).boundary()).isNotSameAs(captured.get(1).boundary());
                    assertThatThrownBy(()->directory.read(175).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
                    assertThat(directory.localActive(175,"c001",1).toCompletableFuture().join()).isTrue();
                    // Actual Main's runner rejects missing business inputs and retires typed database resources.
                    assertThatThrownBy(()->context.getBean(org.springframework.boot.ApplicationRunner.class).run(new org.springframework.boot.DefaultApplicationArguments()))
                        .hasMessageContaining("Native runtime not installed");
                    assertThat(captured).allSatisfy(sql->assertThat(sql.pools().closed()).isTrue());
                });
        }
    }
    static NativeControlDatabaseEnrollment.Database database(String url){
        return new NativeControlDatabaseEnrollment.Database(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword(),
            Map.of(DbClass.RECOVERY,2,DbClass.CRITICAL,2),2,2);
    }
}
