package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import com.typesafe.config.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import javax.net.ssl.*;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.JoinSeedNodes;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.http.javadsl.ConnectionContext;
import org.apache.pekko.management.javadsl.PekkoManagement;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** Actual Main, TLS remoting and management; identities and loopback are TEST_ONLY. */
class NativeActorProcessIT {
    @TempDir Path temporary;
    @Test void managementBindFailureRetiresTheCreatedProcessBeforeBusinessComposition()throws Exception {
        try(var occupied=new ServerSocket(0,1,InetAddress.getLoopbackAddress())){
            var enrollment=enrollment(occupied.getLocalPort());
            var observed=new AtomicReference<ActorSystem<?>>();var remotingPort=new AtomicInteger();
            var app=application(enrollment);
            app.addInitializers(context->context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor(){
                @Override public Object postProcessAfterInitialization(Object bean,String name){
                    if(bean instanceof NativeActorProcess process){
                        observed.set(process.system());
                        remotingPort.set(Integer.parseInt(org.apache.pekko.cluster.Cluster.get(Adapter.toClassic(process.system())).selfAddress().port().get().toString()));
                    }
                    return bean;
                }
            }));
            var failure=catchThrowable(()->app.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
            assertThat(failure).hasRootCauseInstanceOf(BindException.class);
            assertThat(observed.get()).as("Main must publish the process owner before management starts").isNotNull();
            assertThat(observed.get().getWhenTerminated().toCompletableFuture()).isCompleted();
            int port=remotingPort.get();
            assertThatThrownBy(()->new Socket("127.0.0.1",port)).isInstanceOf(java.io.IOException.class);
        }
    }
    @Test void failureBeforeCompositionReleasesInstalledNativeRootsBeforeClosingPools()throws Exception {
        var runtime=new DbTestRuntime();
        var process=new NativeActorProcess(enrollment(0));var system=process.system();
        var releaseEntered=new CountDownLatch(1);var releasePhysical=new CompletableFuture<DbOperation.PhysicalCompletion>();
        try {
            try(var c=PgFixture.connection();var q=c.createStatement()){
                q.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");
            }
            var repository=new GroupOwnerRepository(runtime.sql,"c001",1);
            GroupOwnership held=new GroupOwnership(){
                public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){return repository.acquireTracked(group,node,incarnation,operation);}
                public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){return repository.pulseTracked(grant,sequence,operation);}
                public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){
                    var actual=repository.releaseTracked(token);releaseEntered.countDown();
                    return new DbOperation<>(actual.logical(),actual.physicalCompletion().thenCompose(done->releasePhysical));
                }
                public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){return repository.reconcileTracked(group,node,incarnation);}
                public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){return repository.reconcile(group,node,incarnation);}
            };
            PostgresShardLeaseProvider.install(Adapter.toClassic(system),held,"c001",1,UUID.randomUUID(),()->true);
            var lease=org.apache.pekko.coordination.lease.javadsl.LeaseProvider.get(Adapter.toClassic(system)).getLease("managed-test-only-c001-shard-SignalingCallV1-985","signaling.postgres-lease",org.apache.pekko.cluster.Cluster.get(Adapter.toClassic(system)).selfAddress().hostPort());
            assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
            var app=SignalingApplication.application();app.setWebApplicationType(WebApplicationType.NONE);app.setRegisterShutdownHook(false);
            var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
            app.addInitializers(context->{
                defaults.forEach(source->context.getEnvironment().getPropertySources().addLast(source));
                var registry=(org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory();
                owned(registry,"testOnlyOwnedProcess",NativeActorProcess.class,process);
                owned(registry,"testOnlyBoundary",DbBoundary.class,runtime.boundary);
                owned(registry,"testOnlyPools",DbPools.class,runtime.pools);
            });
            var drain=CompletableFuture.runAsync(()->app.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
            assertThat(releaseEntered.await(5,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->drain.get(300,TimeUnit.MILLISECONDS)).as("original native root receipt must precede process and pool closure").isInstanceOf(TimeoutException.class);
            assertThat(runtime.pools.closed()).isFalse();
            releasePhysical.complete(DbOperation.PhysicalCompletion.FINISHED);
            assertThatThrownBy(()->drain.get(20,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("Native runtime not installed: ACTOR");
            assertThat(system.getWhenTerminated().toCompletableFuture()).isCompleted();assertThat(runtime.pools.closed()).isTrue();
            assertThatThrownBy(process::awaitLocalUp).hasRootCauseMessage("Native actor process draining");
            try(var c=PgFixture.connection();var q=c.createStatement();var row=q.executeQuery("SELECT status FROM group_owner WHERE cell_id='c001' AND group_id=985")){
                assertThat(row.next()).isTrue();assertThat(row.getString(1)).as("native root release must commit before pool closure").isEqualTo("RELEASED");
            }
        } finally {
            releasePhysical.complete(DbOperation.PhysicalCompletion.FINISHED);
            process.drain().toCompletableFuture().get(20,TimeUnit.SECONDS);runtime.close();
        }
    }
    @Test void actualMainJoinsThroughNativeKubernetesDiscoveryAndMutualTlsBeforeBusiness()throws Exception {
        var systems=new ArrayList<ActorSystem<Void>>();var managementPorts=new ArrayList<Integer>();
        var enrollment=enrollment(freePort());
        var api=HttpsServer.create(new InetSocketAddress("127.0.0.1",0),8);
        api.setHttpsConfigurator(new HttpsConfigurator(enrollment.managementServerTls()));
        var requests=new AtomicInteger();var apiFailures=new AtomicReference<Throwable>();Process child=null;
        var token=temporary.resolve("TEST_ONLY_k8s_token");Files.writeString(token,"TEST_ONLY_K8S_TOKEN");
        try {
            for(int i=0;i<5;i++){
                int port=freePort();managementPorts.add(port);
                var config=ConfigFactory.parseString("pekko.management.http.port="+port+"\npekko.cluster.roles=[\"signaling-actor\",\"az-"+(i%3==0?"a":i%3==1?"b":"c")+"\"]")
                    .withFallback(enrollment.config()).resolve();
                systems.add(ActorSystem.create(Behaviors.empty(),enrollment.systemName(),PekkoShutdownLifecycle.config(config)));
            }
            var seed=Cluster.get(systems.getFirst()).selfMember().address();
            // Only the existing-cluster fixture joins manually. The child Main has no manual/seed path.
            systems.forEach(system->Cluster.get(system).manager().tell(new JoinSeedNodes(List.of(seed))));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(()->systems.stream().allMatch(system->Cluster.get(system).selfMember().status().equals(MemberStatus.up())));
            try(var occupied=new ServerSocket(0,1,InetAddress.getLoopbackAddress())){
                var failedEnrollment=new NativeActorProcessEnrollment(enrollment.systemName(),enrollment(occupied.getLocalPort()).config(),enrollment.managementServerTls(),enrollment.managementClientTls());
                var failedProcess=new NativeActorProcess(failedEnrollment);
                try {
                    Cluster.get(failedProcess.system()).manager().tell(new JoinSeedNodes(List.of(seed)));
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(()->Cluster.get(failedProcess.system()).selfMember().status().equals(MemberStatus.up()));
                    var gate=failedProcess.start().toCompletableFuture();
                    var error=catchThrowable(()->gate.get(5,TimeUnit.SECONDS));
                    assertThat(error).as("Up alone must not hide original management binding failure").hasRootCauseInstanceOf(BindException.class);
                } finally {
                    var shutdown=org.apache.pekko.actor.CoordinatedShutdown.get(failedProcess.system());
                    for(String phase:List.of("service-unbind","cluster-leave","cluster-exiting","cluster-shutdown","before-actor-system-terminate","actor-system-terminate"))
                        shutdown.addTask(phase,"TEST_ONLY_TRACE",()->{System.out.println("TEST_ONLY_FAILED_BIND_PHASE "+phase);return CompletableFuture.completedFuture(org.apache.pekko.Done.getInstance());});
                    var drain=failedProcess.drain().toCompletableFuture();
                    try{drain.get(12,TimeUnit.SECONDS);}catch(TimeoutException pending){
                        var diagnostic=Path.of("target/native-process-fixtures/TEST_ONLY_parent_threads.log");Files.createDirectories(diagnostic.getParent());
                        new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","jcmd").toString(),Long.toString(ProcessHandle.current().pid()),"Thread.print")
                            .redirectErrorStream(true).redirectOutput(diagnostic.toFile()).start().waitFor(5,TimeUnit.SECONDS);
                        System.out.println("TEST_ONLY_FAILED_BIND_TERMINATED "+failedProcess.system().getWhenTerminated().toCompletableFuture().isDone());
                        drain.get(53,TimeUnit.SECONDS);
                    }
                }
            }
            for(var system:systems){
                var server=ConnectionContext.httpsServer(()->{var engine=enrollment.managementServerTls().createSSLEngine();engine.setUseClientMode(false);engine.setNeedClientAuth(true);engine.setEnabledProtocols(new String[]{"TLSv1.3"});return engine;});
                var client=ConnectionContext.httpsClient(enrollment.managementClientTls());
                ShardingBootstrap.startManagement(system,server,client).toCompletableFuture().get(5,TimeUnit.SECONDS);
            }
            managementPorts.add(enrollment.config().getInt("pekko.management.http.port"));
            String pods="{\"items\":["+managementPorts.stream().map(port->"{\"metadata\":{},\"status\":{\"podIP\":\"127.0.0.1\",\"phase\":\"Running\",\"containerStatuses\":[{\"name\":\"actor\",\"state\":{\"running\":{}},\"ready\":false}]},\"spec\":{\"containers\":[{\"name\":\"actor\",\"ports\":[{\"name\":\"management\",\"containerPort\":"+port+"}]}]}}")
                .collect(java.util.stream.Collectors.joining(","))+"]}";
            api.createContext("/api/v1/namespaces/test-only-c001/pods",exchange->{
                try {
                    assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer TEST_ONLY_K8S_TOKEN");
                    assertThat(URLDecoder.decode(exchange.getRequestURI().getRawQuery(),java.nio.charset.StandardCharsets.UTF_8)).contains("labelSelector=app=webrtc-signaling,plane=actor,cell=c001");
                    requests.incrementAndGet();byte[] body=pods.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
                }catch(Throwable failure){apiFailures.set(failure);}finally{exchange.close();}
            });api.start();
            var config=enrollment.config()
                .withValue("pekko.discovery.kubernetes-api.api-ca-path",ConfigValueFactory.fromAnyRef(cert("ca.crt").getAbsolutePath()))
                .withValue("pekko.discovery.kubernetes-api.api-token-path",ConfigValueFactory.fromAnyRef(token.toString()))
                .withValue("pekko.discovery.kubernetes-api.api-service-host-env-name",ConfigValueFactory.fromAnyRef("TEST_ONLY_K8S_HOST"))
                .withValue("pekko.discovery.kubernetes-api.api-service-port-env-name",ConfigValueFactory.fromAnyRef("TEST_ONLY_K8S_PORT"));
            var configPath=temporary.resolve("TEST_ONLY_child.conf");Files.writeString(configPath,config.root().render(ConfigRenderOptions.concise()));
            var output=temporary.resolve("TEST_ONLY_child.log");
            var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx256m","-cp",System.getProperty("java.class.path"),ActualManagedMain.class.getName(),configPath.toString());
            builder.environment().put("TEST_ONLY_K8S_HOST","localhost");builder.environment().put("TEST_ONLY_K8S_PORT",Integer.toString(api.getAddress().getPort()));
            child=builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
            var diagnostics=Path.of("target/native-process-fixtures");Files.createDirectories(diagnostics);
            boolean stopped=child.waitFor(25,TimeUnit.SECONDS);
            if(!stopped){
                var snapshot=diagnostics.resolve("TEST_ONLY_child_early_threads.log");
                new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","jcmd").toString(),Long.toString(child.pid()),"Thread.print")
                    .redirectErrorStream(true).redirectOutput(snapshot.toFile()).start().waitFor(5,TimeUnit.SECONDS);
                stopped=child.waitFor(65,TimeUnit.SECONDS);
            }
            if(!stopped){
                var threads=temporary.resolve("TEST_ONLY_child_threads.log");
                new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","jcmd").toString(),Long.toString(child.pid()),"Thread.print")
                    .redirectErrorStream(true).redirectOutput(threads.toFile()).start().waitFor(5,TimeUnit.SECONDS);
                Files.copy(threads,diagnostics.resolve("TEST_ONLY_child_threads.log"),StandardCopyOption.REPLACE_EXISTING);
            }
            Files.copy(output,diagnostics.resolve("TEST_ONLY_child_startup.log"),StandardCopyOption.REPLACE_EXISTING);
            String transcript=Files.readString(output);
            assertThat(stopped).withFailMessage("native Kubernetes formation/cleanup did not finish: %s",transcript).isTrue();
            assertThat(child.exitValue()).withFailMessage("TEST_ONLY child failed: %s",transcript).isZero();
            assertThat(transcript).contains("TEST_ONLY_LOCAL_UP_BEFORE_BUSINESS","TEST_ONLY_PROCESS_RETIRED");
            assertThat(requests.get()).isPositive();assertThat(apiFailures.get()).isNull();
        } finally {
            if(child!=null&&child.isAlive()){child.destroyForcibly();child.waitFor(5,TimeUnit.SECONDS);}
            api.stop(0);
            for(var system:systems)system.terminate();
            for(var system:systems)system.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);
        }
    }
    static int freePort()throws Exception {try(var socket=new ServerSocket(0,1,InetAddress.getLoopbackAddress())){return socket.getLocalPort();}}
    public static final class ActualManagedMain {
        public static void main(String[] args)throws Exception {
            var fixture=new NativeActorProcessIT();fixture.temporary=Path.of(args[0]).getParent();
            var pki=fixture.enrollment(0);var config=ConfigFactory.parseFile(Path.of(args[0]).toFile()).resolve();
            var enrollment=new NativeActorProcessEnrollment(pki.systemName(),config,pki.managementServerTls(),pki.managementClientTls());
            var app=fixture.application(enrollment);var observed=new AtomicReference<NativeActorProcess>();
            app.addInitializers(context->{
                context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor(){public Object postProcessAfterInitialization(Object bean,String name){if(bean instanceof NativeActorProcess owner)observed.set(owner);return bean;}});
                var marker=new RootBeanDefinition(String.class,()->{
                    var system=context.getBean(ActorSystem.class);
                    if(!Cluster.get(system).selfMember().status().equals(MemberStatus.up()))throw new AssertionError("TEST_ONLY child not Up");
                    System.out.println("TEST_ONLY_LOCAL_UP_BEFORE_BUSINESS");return "TEST_ONLY";
                });marker.setDependsOn("nativeActorSystem");marker.setDestroyMethodName("");
                ((org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("testOnlyMembershipMarker",marker);
            });
            var failure=catchThrowable(()->app.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
            assertThat(failure).isInstanceOf(NativeRuntimeStartup.missing(SignalingApplication.Plane.ACTOR).getClass());
            assertThat(observed.get()).isNotNull();assertThat(observed.get().system().getWhenTerminated().toCompletableFuture()).isCompleted();
            System.out.println("TEST_ONLY_PROCESS_RETIRED");
        }
    }
    @Test void unavailableDiscoveryNeverFormsAnIsolatedClusterAndRetiresTlsManagementAfterThirtySeconds()throws Exception {
        var input=enrollment(freePort());
        var config=input.config().withValue("pekko.discovery.kubernetes-api.api-service-host-env-name",ConfigValueFactory.fromAnyRef("TEST_ONLY_ABSENT_K8S_HOST"))
            .withValue("pekko.discovery.kubernetes-api.api-service-port-env-name",ConfigValueFactory.fromAnyRef("TEST_ONLY_ABSENT_K8S_PORT"));
        var enrolled=new NativeActorProcessEnrollment(input.systemName(),config,input.managementServerTls(),input.managementClientTls());
        var app=application(enrolled);var owner=new AtomicReference<NativeActorProcess>();var businessEntered=new AtomicInteger();
        app.addInitializers(context->{
            context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor(){public Object postProcessAfterInitialization(Object bean,String name){if(bean instanceof NativeActorProcess process)owner.set(process);return bean;}});
            var marker=new RootBeanDefinition(String.class,()->{businessEntered.incrementAndGet();return "TEST_ONLY";});
            marker.setDependsOn("nativeActorSystem");marker.setDestroyMethodName("");
            ((org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("testOnlyBusinessMarker",marker);
        });
        var launch=CompletableFuture.runAsync(()->app.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
        try {
            var uri=URI.create("https://localhost:"+config.getInt("pekko.management.http.port")+"/bootstrap/seed-nodes");
            try(var client=java.net.http.HttpClient.newBuilder().sslContext(input.managementClientTls()).connectTimeout(Duration.ofSeconds(1)).build()){
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->{
                    try {var response=client.send(java.net.http.HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                        return response.statusCode()==200&&response.sslSession().orElseThrow().getProtocol().equals("TLSv1.3");
                    }catch(java.io.IOException unavailable){return false;}
                });
            }
            assertThat(businessEntered.get()).isZero();assertThat(launch).isNotDone();
            var trust=KeyStore.getInstance("PKCS12");trust.load(null,null);try(var ca=Files.newInputStream(cert("ca.crt").toPath())){trust.setCertificateEntry("TEST_ONLY_CA",CertificateFactory.getInstance("X.509").generateCertificate(ca));}
            var tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);var anonymous=SSLContext.getInstance("TLSv1.3");anonymous.init(null,tm.getTrustManagers(),null);
            try(var client=java.net.http.HttpClient.newBuilder().sslContext(anonymous).connectTimeout(Duration.ofSeconds(1)).build()){
                assertThatThrownBy(()->client.send(java.net.http.HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString()))
                    .as("management must require a cell peer client identity").isInstanceOf(java.io.IOException.class);
            }
            var failure=catchThrowable(()->launch.get(40,TimeUnit.SECONDS));assertThat(failure).hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(businessEntered.get()).isZero();assertThat(owner.get()).isNotNull();
            assertThat(owner.get().system().getWhenTerminated().toCompletableFuture()).isCompleted();
            assertThatThrownBy(()->new Socket("127.0.0.1",config.getInt("pekko.management.http.port"))).isInstanceOf(java.io.IOException.class);
        } finally {if(owner.get()!=null)owner.get().drain().toCompletableFuture().get(65,TimeUnit.SECONDS);}
    }
    @Test void businessFactoryRejectsAnActorSystemDifferentFromTheManagedProcess()throws Exception {
        var process=new NativeActorProcess(enrollment(0));
        var other=ActorSystem.<Void>create(Behaviors.empty(),"TEST_ONLY_other_process",PekkoShutdownLifecycle.config(enrollment(0).config()));
        try {
            var beans=new org.springframework.beans.factory.support.StaticListableBeanFactory();beans.addBean("managed",process);
            var business=new NativeActorBusinessEnrollment(other,"c001",1,1,UUID.randomUUID(),null,null,null,null,null);
            assertThatThrownBy(()->org.springframework.test.util.ReflectionTestUtils.invokeMethod(new NativeActorCompositionConfiguration(),"nativeActorComposition",business,null,null,null,new io.webrtc.signaling.actors.cluster.ClusterReadiness(),
                beans.getBeanProvider(NativeActorSourceEnrollment.class),beans.getBeanProvider(NativeActorSchedulingEnrollment.class),beans.getBeanProvider(NativeActorProcess.class)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Native actor business process differs from managed enrollment");
        } finally {process.drain().toCompletableFuture().get(65,TimeUnit.SECONDS);other.terminate();other.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);}
    }
    org.springframework.boot.SpringApplication application(NativeActorProcessEnrollment enrollment)throws Exception {
        var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
        var app=SignalingApplication.application();app.setWebApplicationType(WebApplicationType.NONE);app.setRegisterShutdownHook(false);
        app.addInitializers(context->{
            defaults.forEach(s->context.getEnvironment().getPropertySources().addLast(s));
            var definition=new RootBeanDefinition(NativeActorProcessEnrollment.class,()->enrollment);definition.setDestroyMethodName("");
            ((org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("testOnlyProcessEnrollment",definition);
        });return app;
    }
    static <T> void owned(org.springframework.beans.factory.support.BeanDefinitionRegistry registry,String name,Class<T> type,T owner){
        var definition=new RootBeanDefinition(type,()->owner);definition.setDestroyMethodName("");registry.registerBeanDefinition(name,definition);
    }
    static java.io.File cert(String name){return new java.io.File(Objects.requireNonNull(NativeActorProcessIT.class.getResource("/test-only-pki/native-process/"+name)).getFile());}
    NativeActorProcessEnrollment enrollment(int managementPort)throws Exception {
        var keys=KeyStore.getInstance("PKCS12");keys.load(null,null);
        var factory=CertificateFactory.getInstance("X.509");
        java.security.cert.Certificate ca,leaf;
        try(var in=Files.newInputStream(cert("ca.crt").toPath())){ca=factory.generateCertificate(in);}
        try(var in=Files.newInputStream(cert("node.crt").toPath())){leaf=factory.generateCertificate(in);}
        String pem=Files.readString(cert("node.key").toPath()).replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","").replaceAll("\\s","");
        var key=KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
        char[] password="TEST_ONLY".toCharArray();keys.setKeyEntry("TEST_ONLY",key,password,new java.security.cert.Certificate[]{leaf,ca});
        var trust=KeyStore.getInstance("PKCS12");trust.load(null,null);trust.setCertificateEntry("TEST_ONLY_CA",ca);
        var keyPath=temporary.resolve("TEST_ONLY_keys.p12");var trustPath=temporary.resolve("TEST_ONLY_trust.p12");
        try(var out=Files.newOutputStream(keyPath)){keys.store(out,password);}try(var out=Files.newOutputStream(trustPath)){trust.store(out,password);}
        var km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());km.init(keys,password);
        var tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);
        var server=SSLContext.getInstance("TLSv1.3");server.init(km.getKeyManagers(),tm.getTrustManagers(),null);
        var config=ConfigFactory.parseString("""
            signaling.cell-id="c001"
            signaling.cluster-fingerprint="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            pekko.remote.artery.canonical.hostname="127.0.0.1"
            pekko.remote.artery.canonical.port=0
            pekko.remote.artery.ssl.config-ssl-engine {
              key-store=%s
              trust-store=%s
              key-store-password="TEST_ONLY"
              key-password="TEST_ONLY"
              trust-store-password="TEST_ONLY"
            }
            pekko.management.http.hostname="127.0.0.1"
            pekko.management.http.port=%d
            pekko.discovery.kubernetes-api.pod-namespace="test-only-c001"
            pekko.discovery.kubernetes-api.pod-label-selector="app=webrtc-signaling,plane=actor,cell=c001"
            pekko.management.cluster.bootstrap.contact-point-discovery.service-name="signaling-c001"
            pekko.cluster.roles=["signaling-actor","az-a"]
            pekko.cluster.jmx.multi-mbeans-in-same-jvm=on
            pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
            pekko.loglevel=WARNING
            """.formatted(ConfigUtil.quoteString(keyPath.toString()),ConfigUtil.quoteString(trustPath.toString()),managementPort))
            .withFallback(ShardingBootstrap.baseConfig()).resolve()
            .withValue("pekko.discovery.kubernetes-api.api-ca-path",ConfigValueFactory.fromAnyRef(cert("ca.crt").getAbsolutePath()))
            .withValue("pekko.discovery.kubernetes-api.api-token-path",ConfigValueFactory.fromAnyRef(temporary.resolve("TEST_ONLY_k8s_token").toString()));
        Files.writeString(temporary.resolve("TEST_ONLY_k8s_token"),"TEST_ONLY_K8S_TOKEN");
        return new NativeActorProcessEnrollment("managed-test-only-c001",config,server,server);
    }
}
