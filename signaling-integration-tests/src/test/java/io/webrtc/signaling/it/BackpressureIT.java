package io.webrtc.signaling.it;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.admission.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.*;
import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class BackpressureIT {
    @Test void bulkSaturationPreservesTerminationAndCreditsOutliveLogicalTimeout(){
        var admission=new EntityAdmission(64,262144,8,32768,4096,33554432);var credits=new java.util.ArrayList<EntityAdmission.Ticket>();
        for(int n=0;n<56;n++)credits.add(admission.acquire("call",EntityAdmission.Priority.NORMAL,1024));
        assertThatThrownBy(()->admission.acquire("call",EntityAdmission.Priority.NORMAL,1)).isInstanceOf(EntityAdmission.Overloaded.class);
        for(int n=0;n<8;n++)credits.add(admission.acquire("call",EntityAdmission.Priority.SAFETY,1024));
        assertThatThrownBy(()->admission.acquire("call",EntityAdmission.Priority.SAFETY,1)).isInstanceOf(EntityAdmission.Overloaded.class);assertThat(admission.count("call")).isEqualTo(64);
        var logical=new CompletableFuture<String>();var physical=new CompletableFuture<Void>();credits.getFirst().releaseAfter(physical);logical.complete("OUTCOME_UNKNOWN");assertThat(admission.count("call")).isEqualTo(64);physical.complete(null);assertThat(admission.count("call")).isEqualTo(63);credits.forEach(EntityAdmission.Ticket::close);assertThat(admission.retainedBytes()).isZero();assertThat(admission.entities()).isZero();
    }
    @Test void entityBytesRejectOneLargeFrameEvenWhenCountHasRoom(){var admission=new EntityAdmission(64,262144,8,32768,4096,33554432);try(var a=admission.acquire("hot",EntityAdmission.Priority.NORMAL,98304);var b=admission.acquire("hot",EntityAdmission.Priority.NORMAL,98304)){assertThatThrownBy(()->admission.acquire("hot",EntityAdmission.Priority.NORMAL,98304)).isInstanceOf(EntityAdmission.Overloaded.class);}assertThat(admission.retainedBytes()).isZero();}
    @Test void oneSlowConsumerCannotBorrowOtherChannelsAndControlIsNeverAcknowledgedWhenDropped(){
        var time=new AtomicLong();var aggregate=new DeliveryCreditController(10,262144,2,32768);var queue=new OutboundQueue(4,16384,Duration.ofSeconds(10),aggregate,time::get);var first=new CompletableFuture<Void>();var sink=new AtomicInteger();
        var delivery=queue.offer(OutboundQueue.Kind.CONTROL,new byte[8192],()->{sink.incrementAndGet();return first;});assertThat(sink).hasValue(1);assertThat(aggregate.retainedBytes()).isEqualTo(8192);
        queue.offer(OutboundQueue.Kind.RELAY,new byte[8192],()->CompletableFuture.completedFuture(null));var rejected=queue.offer(OutboundQueue.Kind.RELAY,new byte[1],()->CompletableFuture.completedFuture(null));assertThat(rejected.toCompletableFuture()).isCompletedExceptionally();
        var other=new OutboundQueue(4,16384,Duration.ofSeconds(10),aggregate,time::get);assertThat(other.offer(OutboundQueue.Kind.CONTROL,new byte[1],()->CompletableFuture.completedFuture(null)).toCompletableFuture()).isCompleted();time.addAndGet(Duration.ofSeconds(11).toNanos());queue.tick();assertThat(delivery.toCompletableFuture()).isCompletedExceptionally();assertThat(queue.closed()).isTrue();assertThat(aggregate.retainedBytes()).isEqualTo(8192);first.complete(null);assertThat(aggregate.retainedBytes()).isZero();other.close();
    }
    @Test void relayCannotConsumeReservedControlDeliveryCredits(){var credit=new DeliveryCreditController(4,65536,1,8192);try(var a=credit.acquire(false,16384);var b=credit.acquire(false,16384);var c=credit.acquire(false,16384)){assertThatThrownBy(()->credit.acquire(false,1)).isInstanceOf(DeliveryCreditController.Overloaded.class);try(var control=credit.acquire(true,8192)){assertThat(credit.retainedBytes()).isEqualTo(57344);}}assertThat(credit.retainedBytes()).isZero();}
    @Test void physicalReceiptSurvivesLogicalUnknownAndEntityTermination()throws Exception {
        var kit=org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit.create();try{
            var user=new io.webrtc.signaling.protocol.Identity.UserId("receipt-user");var logical=new CompletableFuture<io.webrtc.signaling.actors.user.UserCommand.Result>();var physical=new CompletableFuture<io.webrtc.signaling.storage.DbOperation.PhysicalCompletion>();var entered=new CountDownLatch(1);
            var backend=new io.webrtc.signaling.actors.user.UserActor.Backend(){
                public io.webrtc.signaling.storage.DbOperation<io.webrtc.signaling.storage.UserSnapshotService.Snapshot> load(io.webrtc.signaling.protocol.Identity.UserId u,long epoch,Duration budget){return new io.webrtc.signaling.storage.DbOperation<>(CompletableFuture.completedFuture(new io.webrtc.signaling.storage.UserSnapshotService.Snapshot(u,java.util.List.of(),null)),CompletableFuture.completedFuture(io.webrtc.signaling.storage.DbOperation.PhysicalCompletion.FINISHED));}
                public io.webrtc.signaling.storage.DbOperation<io.webrtc.signaling.actors.user.UserCommand.Result> execute(io.webrtc.signaling.actors.user.UserCommand.Operation op,Duration budget){entered.countDown();return new io.webrtc.signaling.storage.DbOperation<>(logical,physical);}
            };
            var shard=kit.<org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding.ShardCommand>createTestProbe();var actor=kit.spawn(io.webrtc.signaling.actors.user.UserActor.create(user,1,shard.ref(),backend,Clock.systemUTC()));
            var call=new io.webrtc.signaling.protocol.Identity.CallId("c001.e1.00000000-0000-0000-0000-000000000001");Instant now=Instant.now();var request=new io.webrtc.signaling.storage.HomeParticipationService.Request(user,call,java.util.UUID.randomUUID(),"a".repeat(64),1,io.webrtc.signaling.storage.HomeParticipationService.Phase.RINGING,new io.webrtc.signaling.storage.HomeParticipationService.Grant("c001",1,1,685,1,1,java.util.UUID.randomUUID(),now,now.plusSeconds(5),"TEST_ONLY"));
            var tracked=TrackedEntityAsk.ask(kit.system(),Duration.ofSeconds(2),io.webrtc.signaling.actors.user.UserCommand.Result.class,(reply,receipt)->actor.tell(new io.webrtc.signaling.actors.user.UserCommand.Mutate(new io.webrtc.signaling.actors.user.UserCommand.Reserve(request),reply,now.plusSeconds(2),1024,receipt)));
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();logical.completeExceptionally(new io.webrtc.signaling.storage.DbOutcomeUnknownException());assertThat(tracked.logical().toCompletableFuture().join().code()).isEqualTo(io.webrtc.signaling.actors.user.UserCommand.Code.UNKNOWN);assertThat(tracked.physicalCompletion().toCompletableFuture()).isNotDone();kit.stop(actor);physical.complete(io.webrtc.signaling.storage.DbOperation.PhysicalCompletion.FINISHED);tracked.physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);
        }finally{kit.shutdownTestKit();}
    }
    @Test void sevenIngressProducersTogetherCannotOverflowOneEntityMailboxOrByteBudget(){
        var held=new java.util.ArrayList<EntityAdmission.Ticket>();int admitted=0;
        for(int node=0;node<7;node++){var admission=EntityAdmission.forIngressProducers(7);for(int slot=0;slot<9;slot++){held.add(admission.acquire("shared-entity",slot<7?EntityAdmission.Priority.NORMAL:EntityAdmission.Priority.SAFETY,1024));admitted++;}assertThatThrownBy(()->admission.acquire("shared-entity",EntityAdmission.Priority.SAFETY,1)).isInstanceOf(EntityAdmission.Overloaded.class);}
        assertThat(admitted).isLessThanOrEqualTo(64);assertThat(admitted+16+2).isLessThan(128);held.forEach(EntityAdmission.Ticket::close);
    }
    @Test void cleanupBeforeReplyCannotRetireCollectorCreditWhileLogicalWorkIsStillPending()throws Exception{
        var kit=org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit.create();
        try{
            var replyRef=new AtomicReference<org.apache.pekko.actor.typed.ActorRef<String>>();
            var receiptRef=new AtomicReference<CompletionReceipt>();
            var tracked=TrackedEntityAsk.ask(kit.system(),Duration.ofSeconds(2),String.class,(reply,receipt)->{replyRef.set(reply);receiptRef.set(receipt);});
            receiptRef.get().signal();
            var barrier=new CompletableFuture<Void>();
            kit.system().scheduler().scheduleOnce(Duration.ofMillis(100),()->barrier.complete(null),kit.system().executionContext());
            barrier.get(1,TimeUnit.SECONDS);
            assertThat(tracked.logical().toCompletableFuture()).isNotDone();
            assertThat(tracked.physicalCompletion().toCompletableFuture()).isNotDone();
            replyRef.get().tell("DONE");
            assertThat(tracked.logical().toCompletableFuture().get(1,TimeUnit.SECONDS)).isEqualTo("DONE");
            tracked.physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);
        }finally{kit.shutdownTestKit();}
    }

    @Test void collectorRetainsCreditUntilSynchronousProofContinuationActuallyReturns()throws Exception{
        var kit=org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit.create();
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try{
            var replyRef=new AtomicReference<org.apache.pekko.actor.typed.ActorRef<String>>();
            var receiptRef=new AtomicReference<CompletionReceipt>();
            var tracked=TrackedEntityAsk.ask(kit.system(),Duration.ofSeconds(2),String.class,(reply,receipt)->{replyRef.set(reply);receiptRef.set(receipt);});
            var continuation=tracked.logical().thenApply(value->{entered.countDown();try{release.await();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new CompletionException(interrupted);}return value;});
            receiptRef.get().signal();replyRef.get().tell("NATIVE_DTO");
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThat(continuation.toCompletableFuture()).isNotDone();
            assertThat(tracked.physicalCompletion().toCompletableFuture()).isNotDone();
            release.countDown();assertThat(continuation.toCompletableFuture().get(1,TimeUnit.SECONDS)).isEqualTo("NATIVE_DTO");
            tracked.physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);
        }finally{release.countDown();kit.shutdownTestKit();}
    }

}
