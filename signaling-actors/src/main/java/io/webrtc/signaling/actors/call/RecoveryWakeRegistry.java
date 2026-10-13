package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.actors.admission.CompletionReceipt;
import io.webrtc.signaling.protocol.ApplicationSerializable;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.AuthoritySql;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.DbOverloadedException;
import io.webrtc.signaling.storage.HomeParticipationService;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorRefResolver;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.MailboxSelector;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Actual hosting-process cleanup witnesses; replies never infer completion from elapsed time.
 */
public final class RecoveryWakeRegistry {
    private static final int MAXIMUM = 2048, MAXIMUM_BYTES = 16 * 1024 * 1024, PER_GROUP = 64;
    private static final long TOMBSTONE = Duration.ofMinutes(5).toNanos();
    private static final Map<ActorSystem<?>, RecoveryWakeRegistry> INSTALLED =
            Collections.synchronizedMap(new WeakHashMap<>());
    private final Map<UUID, Entry> entries = new HashMap<>();
    private final Map<Integer, Integer> groupCounts = new HashMap<>();
    private final ActorRef<Query> owner;
    private final ActorRefResolver resolver;
    private int bytes;
    private RecoveryWakeRegistry(ActorSystem<?> system) {
        resolver = ActorRefResolver.get(system);
        system
                .getWhenTerminated()
                .whenComplete(
                        (done, failure) -> {
                            synchronized (INSTALLED) {
                                INSTALLED.remove(system, this);
                            }
                        });
        owner =
                system.systemActorOf(
                        Behaviors.<Query>receiveMessage(
                                message -> {
                                    receive(message);
                                    return Behaviors.same();
                                }),
                        "native-recovery-witnesses-" + UUID.randomUUID(),
                        MailboxSelector.bounded(128));
    }

    public static RecoveryWakeRegistry get(ActorSystem<?> system) {
        synchronized (INSTALLED) {
            return INSTALLED.computeIfAbsent(system, RecoveryWakeRegistry::new);
        }
    }

    private static boolean matches(Entry entry, CallId call, CompletionReceipt receipt) {
        // ActorRef equality includes the original collector's address/UID incarnation.
        return entry.call.equals(call) && entry.receipt.equals(receipt);
    }

    public synchronized Entry reserve(
            CallId call, AuthoritySql.GroupToken authority, CompletionReceipt receipt) {
        prune();
        var existing = entries.get(receipt.operation());
        if (existing != null) {
            if (!matches(existing, call, receipt))
                throw new IllegalStateException("Mismatched original recovery wake");
            throw new AlreadyRegistered(existing); // An original wake is never submitted twice.
        }
        int size =
                512
                        + call.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + resolver.toSerializationFormat(receipt.recipient()).length()
                        + (authority == null
                        ? 0
                        : authority.node().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        int group = HomeParticipationService.group(call);
        if (size > 8192
                || entries.size() >= MAXIMUM
                || size > MAXIMUM_BYTES - bytes
                || groupCounts.getOrDefault(group, 0) >= PER_GROUP) throw new DbOverloadedException();
        var entry = new Entry(call, authority, receipt, size);
        entries.put(receipt.operation(), entry);
        groupCounts.merge(group, 1, Integer::sum);
        bytes += size;
        receipt
                .recipient()
                .<Object>unsafeUpcast()
                .tell(new CompletionReceipt.PhysicalOwner(receipt.operation(), owner));
        return entry;
    }

    public void reconcile(CallId call, CompletionReceipt original) {
        owner.tell(new Reconcile(call, original));
    }

    private synchronized void receive(Query query) {
        prune();
        var call = query instanceof Reconcile r ? r.call() : ((Acknowledge) query).call();
        var receipt = query instanceof Reconcile r ? r.original() : ((Acknowledge) query).original();
        var entry = entries.get(receipt.operation());
        if (entry == null || !matches(entry, call, receipt)) return;
        if (query instanceof Reconcile) {
            receipt
                    .recipient()
                    .<Object>unsafeUpcast()
                    .tell(new CompletionReceipt.PhysicalOwner(receipt.operation(), owner));
            // A retry only queries the original receipt: never attach another pending callback.
            if (entry.physical.isDone() && !entry.physical.isCompletedExceptionally()) receipt.signal();
        } else if (entry.physical.isDone() && !entry.physical.isCompletedExceptionally())
            remove(receipt.operation(), entry);
    }

    private void prune() {
        long now = System.nanoTime();
        for (var iterator = entries.entrySet().iterator(); iterator.hasNext(); ) {
            var item = iterator.next();
            var entry = item.getValue();
            long completed;
            synchronized (entry) {
                completed = entry.completedAt;
            }
            // Retention removes only independently proven completed witnesses, never UNKNOWN work.
            if (completed != 0
                    && entry.physical.isDone()
                    && !entry.physical.isCompletedExceptionally()
                    && now - completed >= TOMBSTONE) {
                iterator.remove();
                release(entry);
            }
        }
    }

    private void remove(UUID operation, Entry entry) {
        if (entries.remove(operation, entry)) release(entry);
    }

    private void release(Entry entry) {
        bytes -= entry.bytes;
        groupCounts.compute(
                HomeParticipationService.group(entry.call), (key, count) -> count == 1 ? null : count - 1);
    }

    public sealed interface Query extends ApplicationSerializable permits Reconcile, Acknowledge {
    }

    public record Reconcile(CallId call, CompletionReceipt original) implements Query {
    }

    public record Acknowledge(CallId call, CompletionReceipt original) implements Query {
    }

    public static final class AlreadyRegistered extends RuntimeException {
        private final Entry entry;

        private AlreadyRegistered(Entry entry) {
            this.entry = entry;
        }

        public Entry entry() {
            return entry;
        }
    }

    public static final class Entry {
        private final CallId call;
        private final AuthoritySql.GroupToken authority;
        private final CompletionReceipt receipt;
        private final int bytes;
        private final CompletableFuture<DbOperation.PhysicalCompletion> physical =
                new CompletableFuture<>();
        private long completedAt;

        private Entry(
                CallId call, AuthoritySql.GroupToken authority, CompletionReceipt receipt, int bytes) {
            this.call = call;
            this.authority = authority;
            this.receipt = receipt;
            this.bytes = bytes;
        }

        public CompletionStage<DbOperation.PhysicalCompletion> physical() {
            return physical.minimalCompletionStage();
        }

        public void attach(CompletionStage<DbOperation.PhysicalCompletion> original) {
            original.whenComplete(
                    (value, failure) -> {
                        if (failure != null) physical.completeExceptionally(failure);
                        else if (value == null)
                            physical.completeExceptionally(new IllegalStateException("Missing cleanup proof"));
                        else {
                            synchronized (this) {
                                completedAt = System.nanoTime();
                            }
                            physical.complete(value);
                        }
                    });
        }

        public void notStarted() {
            synchronized (this) {
                completedAt = System.nanoTime();
            }
            physical.complete(DbOperation.PhysicalCompletion.NOT_STARTED);
        }
    }
}
