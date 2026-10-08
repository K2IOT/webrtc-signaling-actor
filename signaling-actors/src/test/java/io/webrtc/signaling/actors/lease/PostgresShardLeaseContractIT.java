package io.webrtc.signaling.actors.lease;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.storage.*;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.coordination.lease.LeaseSettings;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
class PostgresShardLeaseContractIT {
    static DbTestRuntime runtime;static GroupOwnerRepository repository;static ScheduledThreadPoolExecutor timer;
    @BeforeAll static void setup()throws Exception {runtime=new DbTestRuntime();repository=new GroupOwnerRepository(runtime.sql,"c001",1);timer=new ScheduledThreadPoolExecutor(2);timer.setRemoveOnCancelPolicy(true);try(var c=PgFixture.connection();var s=c.createStatement()){s.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");}}
    @AfterAll static void close(){timer.shutdownNow();runtime.close();}
    static LeaseSettings settings(int group,long timeoutMs){return LeaseSettings.apply(ConfigFactory.parseString("heartbeat-interval=5s\nheartbeat-timeout=15s\nlease-operation-timeout="+timeoutMs+"ms"),"lease-contract-shard-SignalingCallV1-"+group,"127.0.0.1:2552");}
    static PostgresShardLease lease(int group,GroupOwnership ownership,AtomicLong nanos){return new PostgresShardLease(settings(group,2000),new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","member#clusterUID#podUID"),ownership,timer,()->true,nanos==null?System::nanoTime:nanos::get);}
    @Test void acquireCommitPrecedesGateAndDuplicateAcquireRetainsEpoch() {
        var lease=lease(100,repository,null);assertThat(lease.checkLease()).isFalse();assertThat(lease.acquire().toCompletableFuture().join()).isTrue();var first=lease.currentGrant().orElseThrow();
        assertThat(lease.acquire().toCompletableFuture().join()).isTrue();assertThat(lease.currentGrant().orElseThrow().token()).isEqualTo(first.token());
        assertThat(lease.release().toCompletableFuture().join()).isTrue();assertThat(lease.checkLease()).isFalse();assertThat(repository.reconcile(100,first.token().node(),first.token().incarnation()).toCompletableFuture().join()).isEmpty();
    }
    @Test void orderedPulseReplayAndOutOfOrderCycleNeverExtendOrReset(){
        UUID incarnation=UUID.randomUUID();var acquired=repository.acquire(101,"pulse-node",incarnation,UUID.randomUUID()).toCompletableFuture().join().orElseThrow();UUID operation=UUID.randomUUID();
        var renewed=repository.pulse(acquired,2,operation).toCompletableFuture().join();assertThat(renewed.sequence()).isEqualTo(2);assertThat(repository.pulse(acquired,2,operation).toCompletableFuture().join().leaseUntil()).isEqualTo(renewed.leaseUntil());
        assertThatThrownBy(()->repository.pulse(renewed,1,UUID.randomUUID()).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);repository.release(renewed.token()).toCompletableFuture().join();
    }
    @Test void takeoverFencesOldTokenAndLateReleaseDoesNotModifyNewOwner(){
        var old=repository.acquire(102,"old-node",UUID.randomUUID(),UUID.randomUUID()).toCompletableFuture().join().orElseThrow();assertThat(repository.acquire(102,"new-node",UUID.randomUUID(),UUID.randomUUID()).toCompletableFuture().join()).isEmpty();repository.release(old.token()).toCompletableFuture().join();
        var newer=repository.acquire(102,"new-node",UUID.randomUUID(),UUID.randomUUID()).toCompletableFuture().join().orElseThrow();assertThat(newer.token().epoch()).isGreaterThan(old.token().epoch());assertThat(repository.release(old.token()).toCompletableFuture().join()).isTrue();
        assertThat(repository.reconcile(102,newer.token().node(),newer.token().incarnation()).toCompletableFuture().join()).isPresent();assertThatThrownBy(()->repository.pulse(old,2,UUID.randomUUID()).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);repository.release(newer.token()).toCompletableFuture().join();
    }
    @Test void monotonicExpiryAndClockFailureCloseGateAndNotifyOnce(){
        var nanos=new AtomicLong(System.nanoTime());var loss=new AtomicInteger();var lease=lease(103,repository,nanos);assertThat(lease.acquire(error->loss.incrementAndGet()).toCompletableFuture().join()).isTrue();var grant=lease.currentGrant().orElseThrow();assertThat(repository.reconcile(103,grant.token().node(),grant.token().incarnation()).toCompletableFuture().join()).isPresent();nanos.addAndGet(Duration.ofSeconds(11).toNanos());assertThat(lease.checkLease()).isFalse();lease.invalidate(new IllegalStateException("test pause"));lease.invalidate(new IllegalStateException("duplicate loss"));assertThat(loss.get()).isEqualTo(1);lease.release().toCompletableFuture().join();
        assertThat(repository.reconcile(103,grant.token().node(),grant.token().incarnation()).toCompletableFuture().join()).isEmpty();
    }
    @Test void lostPostCommitAcquisitionAckNeverLateEnablesChildrenAndOrphanIsReleased() {
        var late=new CompletableFuture<Optional<GroupOwnerRepository.Grant>>();var committed=new CompletableFuture<GroupOwnerRepository.Grant>();var delayOnce=new AtomicBoolean(true);
        GroupOwnership delayed=new GroupOwnership(){
            public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){var actual=repository.acquireTracked(group,node,incarnation,operation);if(!delayOnce.getAndSet(false))return actual;actual.logical().thenAccept(result->committed.complete(result.orElseThrow()));return new DbOperation<>(late.minimalCompletionStage(),actual.physicalCompletion());}
            public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){return repository.pulseTracked(grant,sequence,operation);}
            public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){return repository.releaseTracked(token);}
            public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){return repository.reconcile(group,node,incarnation);}
            public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){return repository.reconcileTracked(group,node,incarnation);}
        };
        var lease=new PostgresShardLease(settings(104,200),new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","delayed#clusterUID#podUID"),delayed,timer,()->true,System::nanoTime);
        var acquired=lease.acquire();var grant=committed.join();assertThatThrownBy(()->acquired.toCompletableFuture().join()).hasCauseInstanceOf(DbOutcomeUnknownException.class);assertThat(lease.checkLease()).isFalse();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(()->assertThat(repository.reconcile(104,grant.token().node(),grant.token().incarnation()).toCompletableFuture().join()).isEmpty());
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(()->assertThat(lease.acquire().toCompletableFuture().join()).isTrue());
        var newer=lease.currentGrant().orElseThrow();assertThat(newer.token().incarnation()).isNotEqualTo(grant.token().incarnation());late.complete(Optional.of(grant));assertThat(lease.checkLease()).isTrue();assertThat(lease.currentGrant().orElseThrow().token()).isEqualTo(newer.token());lease.release().toCompletableFuture().join();
    }
    @Test void namespaceRejectsUserLeasesUnknownTypesAndNoncanonicalShardNames(){
        var ns=new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","test#clusterUID#podUID");
        for(String name:List.of("lease-contract-shard-SignalingUserV1-1","other-shard-SignalingCallV1-1","lease-contract-shard-SignalingCallV1-001","lease-contract-shard-SignalingCallV1-1024"))assertThatThrownBy(()->new PostgresShardLease(LeaseSettings.apply(settings(1,2000).leaseConfig(),name,"127.0.0.1:2552"),ns,repository,timer,()->true,System::nanoTime)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void unknownPulseAckImmediatelyClosesGateAndLateAckCannotReviveTenure()throws Exception {
        var late=new CompletableFuture<GroupOwnerRepository.Grant>();var pulseCommit=new CompletableFuture<GroupOwnerRepository.Grant>();var loss=new AtomicInteger();
        GroupOwnership delayed=new GroupOwnership(){
            public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){return repository.acquireTracked(group,node,incarnation,operation);}
            public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){var actual=repository.pulseTracked(grant,sequence,operation);actual.logical().thenAccept(pulseCommit::complete);return new DbOperation<>(late.minimalCompletionStage(),actual.physicalCompletion());}
            public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){return repository.releaseTracked(token);}
            public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){return repository.reconcile(group,node,incarnation);}
            public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){return repository.reconcileTracked(group,node,incarnation);}
        };
        var lease=new PostgresShardLease(settings(107,500),new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","pulse-delay#clusterUID#podUID"),delayed,timer,()->true,System::nanoTime);
        assertThat(lease.acquire(error->loss.incrementAndGet()).toCompletableFuture().join()).isTrue();var committed=pulseCommit.get(7,TimeUnit.SECONDS);assertThat(committed.sequence()).isEqualTo(2);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(()->{assertThat(lease.checkLease()).isFalse();assertThat(loss.get()).isEqualTo(1);});
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(()->assertThat(repository.reconcile(107,committed.token().node(),committed.token().incarnation()).toCompletableFuture().join()).isEmpty());
        late.complete(committed);assertThat(lease.checkLease()).isFalse();assertThat(loss.get()).isEqualTo(1);
    }
    @Test void unhealthyClockPreventsAcquisitionAndClosesExistingGate(){
        var healthy=new AtomicBoolean(false);var loss=new AtomicInteger();var lease=new PostgresShardLease(settings(108,2000),new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","clock#clusterUID#podUID"),repository,timer,healthy::get,System::nanoTime);
        assertThatThrownBy(()->lease.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        healthy.set(true);assertThat(lease.acquire(error->loss.incrementAndGet()).toCompletableFuture().join()).isTrue();healthy.set(false);assertThat(lease.checkLease()).isFalse();lease.invalidate(new IllegalStateException("clock unhealthy"));assertThat(loss.get()).isEqualTo(1);lease.release().toCompletableFuture().join();
    }
    @Test void publicPekkoProviderLoadsAdapterAndRequiresInstalledDependencies()throws Exception {
        var config=ConfigFactory.parseString("""
            pekko.actor.provider=cluster
            pekko.remote.artery.canonical.hostname="127.0.0.1"
            pekko.remote.artery.canonical.port=0
            signaling.lease-placement-mailbox { mailbox-type="org.apache.pekko.dispatch.NonBlockingBoundedMailbox", mailbox-capacity=2048, mailbox-push-timeout-time=0ms }
            signaling.postgres-lease {
              lease-class="io.webrtc.signaling.actors.lease.PostgresShardLease"
              heartbeat-interval=5s
              heartbeat-timeout=15s
              lease-operation-timeout=2s
            }
            """).withFallback(ConfigFactory.load());
        var system=org.apache.pekko.actor.ActorSystem.create("lease-provider-test",config);
        try {
            var address=org.apache.pekko.cluster.Cluster.get(system).selfAddress();String host=address.hostPort();
            var provider=org.apache.pekko.coordination.lease.javadsl.LeaseProvider.get(system);
            assertThatThrownBy(()->provider.getLease("lease-provider-test-shard-SignalingCallV1-106","signaling.postgres-lease",host)).isInstanceOf(IllegalStateException.class);
            PostgresShardLeaseProvider.install(system,repository,"c001",1,UUID.randomUUID(),()->true);
            var adapter=provider.getLease("lease-provider-test-shard-SignalingCallV1-106","signaling.postgres-lease",host);assertThat(adapter).isInstanceOf(PostgresShardLease.class);
            assertThat(adapter.acquire().toCompletableFuture().join()).isTrue();
            PostgresShardLeaseProvider.shedNewAcquisition(system);assertThat(adapter.acquire().toCompletableFuture().join()).isTrue();
            var late=provider.getLease("lease-provider-test-shard-SignalingCallV1-982","signaling.postgres-lease",host);
            assertThatThrownBy(()->late.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            PostgresShardLeaseProvider.drain(system).toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertThat(((PostgresShardLease)adapter).currentGrant()).isEmpty();
            assertThatThrownBy(()->adapter.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(adapter.release().toCompletableFuture().join()).isTrue();
        }finally{system.terminate();system.getWhenTerminated().toCompletableFuture().get(10,TimeUnit.SECONDS);}
    }
    @Test void drainWaitsForPositiveNativeReleasePhysicalCleanupAndPermanentlyStopsAcquisition(){
        var cleanup=new CompletableFuture<DbOperation.PhysicalCompletion>();
        GroupOwnership delayed=new GroupOwnership(){
            public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){return repository.acquireTracked(group,node,incarnation,operation);}
            public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){return repository.pulseTracked(grant,sequence,operation);}
            public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){var actual=repository.releaseTracked(token);assertThat(actual.logical().toCompletableFuture().join()).isTrue();actual.physicalCompletion().toCompletableFuture().join();return new DbOperation<>(CompletableFuture.completedFuture(true),cleanup);}
            public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){return repository.reconcile(group,node,incarnation);}
            public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){return repository.reconcileTracked(group,node,incarnation);}
        };
        var lease=lease(111,delayed,null);assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
        try {
            var drain=lease.drain().toCompletableFuture();assertThat(lease.checkLease()).isFalse();assertThat(drain).isNotDone();assertThat(lease.drain().toCompletableFuture()).isNotDone();
            // Framework PostStop and placement loss can repeat the same native release.
            assertThat(lease.release().toCompletableFuture()).as("repeat release preserves original committed reply and physical tail").isCompletedWithValue(true);
            assertThat(drain).isNotDone();
            assertThatThrownBy(()->lease.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            cleanup.complete(DbOperation.PhysicalCompletion.FINISHED);drain.join();assertThat(lease.drain().toCompletableFuture()).isCompleted();
        } finally {cleanup.complete(DbOperation.PhysicalCompletion.FINISHED);}
    }

    @Test void duplicateFrameworkReleaseRetainsPendingReplyAndOriginalPhysicalReceipt()throws Exception{
        var reply=new CompletableFuture<Boolean>();var physical=new CompletableFuture<DbOperation.PhysicalCompletion>();var releases=new AtomicInteger();
        GroupOwnership delayed=new GroupOwnership(){
            public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){return repository.acquireTracked(group,node,incarnation,operation);}
            public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){return repository.pulseTracked(grant,sequence,operation);}
            public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){
                releases.incrementAndGet();var actual=repository.releaseTracked(token);
                assertThat(actual.logical().toCompletableFuture().join()).isTrue();actual.physicalCompletion().toCompletableFuture().join();
                return new DbOperation<>(reply,physical);
            }
            public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){return repository.reconcile(group,node,incarnation);}
            public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){return repository.reconcileTracked(group,node,incarnation);}
        };
        var lease=lease(114,delayed,null);assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
        try {
            var original=lease.release().toCompletableFuture();var repeated=lease.release().toCompletableFuture();
            assertThat(original).as("duplicate framework callback must preserve original pending reply").isNotDone();assertThat(repeated).isNotDone();
            var drain=lease.drain().toCompletableFuture();assertThat(drain).isNotDone();assertThat(releases).hasValue(1);
            reply.complete(true);assertThat(original.get(1,TimeUnit.SECONDS)).isTrue();assertThat(repeated.get(1,TimeUnit.SECONDS)).isTrue();
            assertThat(drain).as("logical COMMIT acknowledgment cannot release original physical credit").isNotDone();
            physical.complete(DbOperation.PhysicalCompletion.FINISHED);drain.get(2,TimeUnit.SECONDS);
            assertThat(releases).hasValue(1);assertThat(lease.checkLease()).isFalse();
        } finally {reply.complete(true);physical.complete(DbOperation.PhysicalCompletion.FINISHED);}
    }

    @Test void reconciliationReadRetainsPhysicalCreditBeforeMatchedReleaseAndDrain()throws Exception{
        var acquireReply=new CompletableFuture<Optional<GroupOwnerRepository.Grant>>();var readCleanup=new CompletableFuture<DbOperation.PhysicalCompletion>();var readSeen=new CompletableFuture<Void>();var releases=new AtomicInteger();
        GroupOwnership delayed=new GroupOwnership(){
            public DbOperation<Optional<GroupOwnerRepository.Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){var actual=repository.acquireTracked(group,node,incarnation,operation);actual.logical().toCompletableFuture().join();return new DbOperation<>(acquireReply,actual.physicalCompletion());}
            public DbOperation<GroupOwnerRepository.Grant> pulseTracked(GroupOwnerRepository.Grant grant,long sequence,UUID operation){return repository.pulseTracked(grant,sequence,operation);}
            public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){releases.incrementAndGet();return repository.releaseTracked(token);}
            public CompletionStage<Optional<GroupOwnerRepository.Grant>> reconcile(int group,String node,UUID incarnation){var actual=repository.reconcile(group,node,incarnation);readSeen.complete(null);return actual;}
            public DbOperation<Optional<GroupOwnerRepository.Grant>> reconcileTracked(int group,String node,UUID incarnation){var actual=repository.reconcileTracked(group,node,incarnation);actual.logical().toCompletableFuture().join();actual.physicalCompletion().toCompletableFuture().join();readSeen.complete(null);return new DbOperation<>(actual.logical(),readCleanup);}
        };
        var lease=new PostgresShardLease(settings(112,500),new PostgresShardLease.Namespace("lease-contract","c001",1,"127.0.0.1:2552","reconcile-physical#clusterUID#podUID"),delayed,timer,()->true,System::nanoTime);
        assertThatThrownBy(()->lease.acquire().toCompletableFuture().join()).hasCauseInstanceOf(DbOutcomeUnknownException.class);readSeen.get(2,TimeUnit.SECONDS);
        var drain=lease.drain().toCompletableFuture();assertThatThrownBy(()->drain.get(500,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);assertThat(releases).hasValue(0);
        readCleanup.complete(DbOperation.PhysicalCompletion.FINISHED);drain.get(3,TimeUnit.SECONDS);assertThat(releases).hasValue(1);assertThat(lease.checkLease()).isFalse();
    }

    @Test void sheddingNewAcquisitionKeepsHeldRootPulsesUntilFrameworkHandoff()throws Exception {
        var held=lease(980,repository,null);assertThat(held.acquire().toCompletableFuture().join()).isTrue();var first=held.currentGrant().orElseThrow();
        held.shedNewAcquisition();assertThat(held.acquire().toCompletableFuture().join()).isTrue();assertThat(held.checkLease()).isTrue();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(7)).until(()->held.currentGrant().map(g->g.sequence()>first.sequence()).orElse(false));
        assertThat(held.currentGrant().orElseThrow().token()).isEqualTo(first.token());
        assertThat(held.release().toCompletableFuture().join()).isTrue();
        assertThatThrownBy(()->held.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        var fresh=lease(981,repository,null);fresh.shedNewAcquisition();assertThatThrownBy(()->fresh.acquire().toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        fresh.drain().toCompletableFuture().get(1,TimeUnit.SECONDS);
    }

}
