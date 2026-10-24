package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.actors.cluster.CallMessage;
import io.webrtc.signaling.protocol.ApplicationSerializable;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Snapshot;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One admitted transaction per incarnation, including its physical cleanup after a logical timeout.
 */
public final class CallActor extends AbstractBehavior<CallMessage> {
    private final ParticipantRenewal renewal;
    private final RelayHandler relayHandler;
    private final Consumer<CallId> invalidateRelay;
    private final UUID incarnation = UUID.randomUUID();
    private final CallId call;
    private final Backend backend;
    private final Supplier<Optional<AuthoritySql.GroupToken>> gate;
    private final Clock clock;
    private final TimerScheduler<CallMessage> timers;
    private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final ArrayDeque<CallMessage> queue = new ArrayDeque<>();
    private final List<ActorRef<Optional<Snapshot>>> observers = new ArrayList<>();
    private long hydrationEnd;
    private int hydrationAttempts;
    private boolean retryHydration;
    private Optional<Snapshot> snapshot = Optional.empty();
    private Pending pending;
    private int bytes;
    private boolean ready, unknown, stopping, passivating, renewalDue;
    private boolean renewalStarted;
    private long renewalLastStart, renewalNextDue;
    private int renewalRetries;
    private ParticipantRenewal.Reconciliation renewalRepair;

    private CallActor(
            ActorContext<CallMessage> c,
            TimerScheduler<CallMessage> t,
            CallId call,
            Backend backend,
            Supplier<Optional<AuthoritySql.GroupToken>> gate,
            Clock clock,
            ActorRef<ClusterSharding.ShardCommand> shard,
            Consumer<CallId> invalidateRelay,
            ParticipantRenewal renewal,
            RelayHandler relayHandler) {
        super(c);
        this.renewal = renewal;
        this.relayHandler = Objects.requireNonNull(relayHandler);
        this.invalidateRelay = Objects.requireNonNull(invalidateRelay);
        this.call = Objects.requireNonNull(call);
        this.backend = Objects.requireNonNull(backend);
        this.gate = Objects.requireNonNull(gate);
        this.clock = Objects.requireNonNull(clock);
        timers = t;
        this.shard = shard;
        var token = current();
        if (token.isEmpty()) {
            unknown = true;
            ready = true;
            return;
        }
        pending = new Pending(null, token.get(), Purpose.HYDRATE);
        submit(() -> backend.loadCold(call, pending.token, Duration.ofSeconds(2)));
    }

    private static void check(Instant deadline, int bytes) {
        Objects.requireNonNull(deadline);
        if (bytes < 1 || bytes > 96 * 1024) throw new IllegalArgumentException("Invalid command size");
    }

    public static Behavior<CallMessage> create(
            CallId call, Backend backend, Supplier<Optional<AuthoritySql.GroupToken>> gate, Clock clock) {
        return create(call, backend, gate, clock, null);
    }

    public static Behavior<CallMessage> create(
            CallId call,
            Backend backend,
            Supplier<Optional<AuthoritySql.GroupToken>> gate,
            Clock clock,
            ActorRef<ClusterSharding.ShardCommand> shard) {
        return create(call, backend, gate, clock, shard, c -> {
        });
    }

    public static Behavior<CallMessage> create(
            CallId call,
            Backend backend,
            Supplier<Optional<AuthoritySql.GroupToken>> gate,
            Clock clock,
            ActorRef<ClusterSharding.ShardCommand> shard,
            Consumer<CallId> invalidateRelay) {
        return create(call, backend, gate, clock, shard, invalidateRelay, null);
    }

    public static Behavior<CallMessage> create(
            CallId call,
            Backend backend,
            Supplier<Optional<AuthoritySql.GroupToken>> gate,
            Clock clock,
            ActorRef<ClusterSharding.ShardCommand> shard,
            Consumer<CallId> invalidateRelay,
            ParticipantRenewal renewal) {
        return create(call, backend, gate, clock, shard, invalidateRelay, renewal, RelayHandler.UNAVAILABLE);
    }

    /**
     * {@code relayHandler} runs relay frames on this node; it is reached only while this actor holds
     * the call's ownership group, the node-local authority every relay authorization checks.
     */
    public static Behavior<CallMessage> create(
            CallId call,
            Backend backend,
            Supplier<Optional<AuthoritySql.GroupToken>> gate,
            Clock clock,
            ActorRef<ClusterSharding.ShardCommand> shard,
            Consumer<CallId> invalidateRelay,
            ParticipantRenewal renewal,
            RelayHandler relayHandler) {
        return Behaviors.withTimers(
                t ->
                        Behaviors.setup(
                                c ->
                                        new CallActor(
                                                c,
                                                t,
                                                call,
                                                backend,
                                                gate,
                                                clock,
                                                shard,
                                                invalidateRelay,
                                                renewal,
                                                relayHandler)));
    }

    private static int size(CallMessage r) {
        return switch (r) {
            case GrantToHome g -> g.encodedBytes();
            case Progress p -> p.encodedBytes();
            case Execute e -> e.encodedBytes();
            case WakeCall w -> w.encodedBytes();
            default -> 128;
        };
    }

    private static Instant deadline(CallMessage r) {
        return switch (r) {
            case GrantToHome g -> g.deadline();
            case Progress p -> p.deadline();
            case Execute e -> e.deadline();
            case WakeCall w -> w.deadline();
            default -> throw new IllegalArgumentException("Not queued work");
        };
    }

    private static io.webrtc.signaling.actors.admission.CompletionReceipt receipt(
            CallMessage request) {
        if (request == null) return null;
        return switch (request) {
            case GrantToHome g -> g.receipt();
            case Progress p -> p.receipt();
            case Execute e -> e.receipt();
            case WakeCall w -> w.receipt();
            default -> null;
        };
    }

    private static Throwable unwrap(Throwable e) {
        while (e instanceof CompletionException && e.getCause() != null) e = e.getCause();
        return e;
    }

    private static String classify(Throwable e) {
        e = unwrap(e);
        if (e instanceof AuthoritySql.FencedException
                || e instanceof CallCommandService.AuthorizationRejected) return "FENCED";
        if (e instanceof AuthoritySql.RetryableConflict || e instanceof DbOverloadedException)
            return "OVERLOADED";
        if (e instanceof IllegalArgumentException) return "INVALID";
        return "UNKNOWN";
    }

    @Override
    public Receive<CallMessage> createReceive() {
        return newReceiveBuilder()
                .onMessage(GrantToHome.class, this::enqueue)
                .onMessage(Progress.class, this::enqueue)
                .onMessage(Execute.class, this::enqueue)
                .onMessage(WakeCall.class, this::enqueue)
                .onMessage(Relay.class, this::relay)
                .onMessage(
                        ReconcileWake.class,
                        r -> {
                            if (call.equals(r.call()) && r.deadline().isAfter(clock.instant()))
                                RecoveryWakeRegistry.get(getContext().getSystem()).reconcile(call, r.original());
                            return this;
                        })
                .onMessage(GetSnapshot.class, this::observe)
                .onMessage(Completed.class, this::completed)
                .onMessage(Cleaned.class, this::cleaned)
                .onMessage(Expire.class, this::expire)
                .onMessage(RetryHydration.class, this::retryHydration)
                .onMessage(Renew.class, r -> renewParticipants())
                .onMessage(Retire.class, r -> retire())
                .onMessage(Stop.class, r -> stop())
                .build();
    }

    private Optional<AuthoritySql.GroupToken> current() {
        return gate.get()
                .filter(
                        t ->
                                call.coordinatorCell().equals(t.cell())
                                        && t.hashVersion() == 1
                                        && t.group() == HomeParticipationService.group(call));
    }

    private Behavior<CallMessage> enqueue(CallMessage request) {
        if (current().isEmpty()) {
            reject(request, "FENCED");
            return this;
        }
        if (stopping || passivating || unknown) {
            reject(request, "UNAVAILABLE");
            return this;
        }
        if (!deadline(request).isAfter(clock.instant())) {
            reject(request, "EXPIRED");
            return this;
        }
        if (request instanceof GrantToHome g && !call.equals(g.request().call())
                || request instanceof Progress p
                && (!call.equals(p.transition().call())
                || !current().get().equals(p.transition().group()))
                || request instanceof Execute e
                && e.command().callId() != null
                && !call.equals(e.command().callId())
                || request instanceof WakeCall w && !call.equals(w.call())) {
            reject(request, "FENCED");
            return this;
        }
        if (queue.size() + (pending == null ? 0 : 1) >= 64 || bytes + size(request) > 256 * 1024) {
            reject(request, "OVERLOADED");
            return this;
        }
        queue.add(request);
        bytes += size(request);
        start();
        return this;
    }

    private void start() {
        while (ready && pending == null && !queue.isEmpty() && !unknown && !stopping) {
            var r = queue.remove();
            var token = current();
            if (token.isEmpty() || !deadline(r).isAfter(clock.instant())) {
                bytes -= size(r);
                reject(r, token.isEmpty() ? "FENCED" : "EXPIRED");
                continue;
            }
            if (r instanceof Progress p && !p.transition().group().equals(token.get())) {
                bytes -= size(r);
                reject(r, "FENCED");
                continue;
            }
            pending = new Pending(r, token.get(), Purpose.WORK);
            Duration remaining = Duration.between(clock.instant(), deadline(r));
            Duration budget =
                    remaining.compareTo(Duration.ofSeconds(2)) > 0 ? Duration.ofSeconds(2) : remaining;
            submit(
                    () ->
                            switch (r) {
                                case GrantToHome g -> backend.grant(
                                        g.request(),
                                        g.action(),
                                        token.get(),
                                        snapshot.map(Snapshot::version).orElse(0L),
                                        budget);
                                case Progress p -> backend.progress(p.transition(), budget);
                                case Execute e -> backend.command(
                                        e.command(),
                                        token.get(),
                                        snapshot.map(Snapshot::version).orElse(0L),
                                        e.proof(),
                                        budget);
                                case WakeCall w -> backend.load(call, token.get(), budget);
                                default -> throw new IllegalArgumentException("Unknown call work");
                            });
        }
    }

    private void submit(Supplier<DbOperation<?>> action) {
        UUID operation = pending.id;
        var token = pending.token;
        var receipt = receipt(pending.request);
        RecoveryWakeRegistry.Entry witness = null;
        try (var traceScope =
                     io.webrtc.signaling.observability.TraceContext.activate(
                             switch (pending.request) {
                                 case null -> null; // Hydration, expiry and renewal are internal physical owners.
                                 case Execute execute -> execute.trace();
                                 case GrantToHome grant -> grant.trace();
                                 case Progress progress -> progress.trace();
                                 default -> null;
                             })) {
            if (pending.request instanceof WakeCall && receipt != null)
                witness = RecoveryWakeRegistry.get(getContext().getSystem()).reserve(call, token, receipt);
            var handle = action.get();
            if (witness != null) witness.attach(handle.physicalCompletion());
            if (receipt != null)
                handle
                        .physicalCompletion()
                        .whenComplete(
                                (done, error) -> {
                                    if (error == null) receipt.signal();
                                });
            getContext()
                    .pipeToSelf(
                            handle.logical(), (v, e) -> new Completed(incarnation, operation, token, v, e));
            getContext()
                    .pipeToSelf(
                            handle.physicalCompletion(), (v, e) -> new Cleaned(incarnation, operation, e));
        } catch (RuntimeException e) {
            if (e instanceof RecoveryWakeRegistry.AlreadyRegistered duplicate) {
                getContext()
                        .pipeToSelf(
                                duplicate.entry().physical(),
                                (v, failure) -> new Cleaned(incarnation, operation, failure));
                duplicate
                        .entry()
                        .physical()
                        .whenComplete(
                                (v, failure) -> {
                                    if (failure == null) receipt.signal();
                                });
                getContext()
                        .getSelf()
                        .tell(new Completed(incarnation, operation, token, null, new DbOverloadedException()));
                return;
            }
            String code = classify(e);
            if (!code.equals("UNKNOWN")) {
                if (witness != null) witness.notStarted();
                if (receipt != null) receipt.signal();
                getContext().getSelf().tell(new Cleaned(incarnation, operation, null));
            }
            getContext().getSelf().tell(new Completed(incarnation, operation, token, null, e));
        }
    }

    private Behavior<CallMessage> completed(Completed e) {
        if (!incarnation.equals(e.actor())
                || pending == null
                || !pending.id.equals(e.operation())
                || !pending.token.equals(e.token())
                || pending.logical) return this;
        pending.logical = true;
        if (e.error() != null) {
            pending.failure = unwrap(e.error());
            String code = classify(e.error());
            retryHydration =
                    pending.hydration()
                            && code.equals("OVERLOADED")
                            && hydrationEnd != 0
                            && System.nanoTime() - hydrationEnd < 0
                            && hydrationAttempts < 16;
            boolean knownRenewalOverload =
                    pending.renewal()
                            && code.equals("OVERLOADED")
                            && current().filter(e.token()::equals).isPresent();
            // Null request means internal work, not hydration. A known renewal overload keeps the
            // incarnation live, but no replacement cycle is started before this owner's real cleanup.
            if (pending.purpose == Purpose.RECONCILE_RENEW
                    || pending.purpose == Purpose.RECONCILE_HYDRATE) unknown = true;
            else if (!knownRenewalOverload)
                unknown = !retryHydration && (code.equals("UNKNOWN") || pending.request == null);
            if (pending.request != null) reply(pending.request, code);
        } else if (!current().filter(e.token()::equals).isPresent()) {
            unknown = true;
            if (pending.request != null) reply(pending.request, "UNKNOWN");
        } else
            try {
                if (e.value() instanceof CoordinatorGrantService.Issued value) {
                    if (pending.request instanceof GrantToHome g)
                        g.replyTo()
                                .tell(
                                        new GrantReply(
                                                "GRANTED", value, backend.sealGrant(g.request(), g.destination(), value)));
                } else if (e.value() instanceof ParticipantRenewal.Result result) {
                    if (!pending.renewal()) throw new IllegalArgumentException("Unexpected renewal outcome");
                    if (pending.purpose == Purpose.RECONCILE_RENEW) {
                        unknown = false;
                        renewalRepair = null;
                    }
                    renewalRetries = 0;
                    // A live near-expiry lease may need the next real root grant before the normal cadence.
                    if (result.retryAfter() != null) {
                        renewalNextDue =
                                Math.min(renewalNextDue, renewalLastStart + result.retryAfter().toNanos());
                        scheduleRenewal();
                    }
                } else if (e.value() instanceof Optional<?> loaded) {
                    setSnapshot(loaded.map(v -> (Snapshot) v));
                    ready = true;
                    hydrationEnd = 0;
                    hydrationAttempts = 0;
                    retryHydration = false;
                    if (pending.request instanceof WakeCall w) w.replyTo().tell(snapshot.isPresent());
                } else if (e.value() instanceof CallWorkflowService.Outcome value) {
                    setSnapshot(Optional.of(value.snapshot()));
                    if (pending.request instanceof Progress p) p.replyTo().tell(value);
                } else if (e.value() instanceof CallCommandService.Outcome value) {
                    if (value.status().equals("FINAL")
                            && (Set.of("TERMINAL", "INVALIDATED").contains(value.state())
                            || Set.of("RESUMED", "NEGOTIATION_GRANTED").contains(value.code())))
                        invalidateRelay.accept(call);
                    if (pending.request instanceof Execute r)
                        r.replyTo()
                                .tell(
                                        value); /* Re-read primary after the command before serving another mutation. */
                    ready = false;
                } else throw new IllegalArgumentException("Invalid committed DTO");
            } catch (RuntimeException invalid) {
                pending.failure = invalid;
                unknown = true;
                if (pending.request != null) reply(pending.request, "UNKNOWN");
            }
        if (ready || unknown) {
            observers.forEach(ref -> ref.tell(snapshot));
            observers.clear();
        }
        finish();
        return next();
    }

    private void setSnapshot(Optional<Snapshot> value) {
        if (value.isPresent()) {
            Snapshot s = value.get();
            if (!call.equals(s.callId())
                    || snapshot.isPresent() && s.version() < snapshot.get().version())
                throw new IllegalArgumentException("Stale hydration");
        }
        if (value.isPresent()
                && (value.get().terminalAt() != null
                || Set.of("TERMINAL", "INVALIDATED").contains(value.get().state())
                || snapshot.isPresent()
                && (value.get().negotiationId() != snapshot.get().negotiationId()
                || !Objects.equals(value.get().caller(), snapshot.get().caller())
                || !Objects.equals(value.get().winner(), snapshot.get().winner()))))
            invalidateRelay.accept(call);
        snapshot = value;
        if (renewal != null && snapshot.isPresent() && snapshot.get().terminalAt() == null) {
            if (!renewalStarted) renewalDue = true; // First guarded hydration pulses without timer delay.
            else {
                renewalNextDue = Math.min(renewalNextDue, renewalLastStart + renewalPeriod().toNanos());
                if (System.nanoTime() - renewalNextDue >= 0) renewalDue = true;
                scheduleRenewal();
            }
        } else {
            renewalDue = false;
            timers.cancel(Renew.INSTANCE);
        }
        timers.cancel("deadline");
        timers.cancel(Retire.INSTANCE);
        if (snapshot.isPresent()) {
            var s = snapshot.get();
            if (s.terminalAt() != null) timers.startSingleTimer(Retire.INSTANCE, Duration.ofMinutes(2));
            else {
                Duration d = DurableDeadlineScheduler.delay(s, clock);
                if (d != null)
                    timers.startSingleTimer(
                            "deadline", new Expire(incarnation, pending.token, s.version()), d);
            }
        }
    }

    private Behavior<CallMessage> cleaned(Cleaned e) {
        if (incarnation.equals(e.actor()) && pending != null && pending.id.equals(e.operation())) {
            if (e.error() != null) {
                unknown = true;
                return this; // Failed cleanup cannot retire the incarnation's admitted credit.
            }
            pending.physical = true;
            finish();
        }
        return next();
    }

    private void finish() {
        if (pending == null || !pending.logical || !pending.physical) return;
        var settled = pending;
        if (settled.request != null) bytes -= size(settled.request);
        pending = null;
        if (!stopping && current().filter(settled.token::equals).isPresent()) {
            if (settled.renewal() && settled.failure instanceof ParticipantRenewal.Uncertain uncertain) {
                renewalRepair = uncertain.reconciliation();
                unknown = true;
                ready = false;
                finishUnavailable();
                if (settled.purpose == Purpose.RECONCILE_RENEW) retryRenewalAfterSettlement();
                else warmHydration(true);
                return;
            }
            if (settled.purpose == Purpose.RECONCILE_HYDRATE && settled.failure == null) {
                if (snapshot.isPresent() && snapshot.get().terminalAt() == null) {
                    reconcileRenewal(settled.token);
                    return;
                }
                // The guarded load can prove durable terminal/absence; no participant write is needed.
                renewalRepair = null;
                unknown = false;
            }
            if (settled.renewal()
                    && settled.failure != null
                    && classify(settled.failure).equals("OVERLOADED")) {
                retryRenewalAfterSettlement();
                if (!unknown) start();
                return;
            }
            if (settled.purpose == Purpose.RECONCILE_HYDRATE
                    && settled.failure != null
                    && (classify(settled.failure).equals("OVERLOADED")
                    || classify(settled.failure).equals("UNKNOWN"))) {
                if (!retryHydration) {
                    retryRenewalAfterSettlement();
                    return;
                }
            }
        } else renewalRepair = null;
        if ((unknown && !retryHydration) || stopping) {
            retryHydration = false;
            // A definitive guard failure cannot be retried as a healthy renewal.
            renewalRepair = null;
            finishUnavailable();
        } else if (retryHydration) {
            retryHydration = false;
            var token = current();
            if (token.isEmpty()) {
                unknown = true;
                renewalRepair = null;
                finishUnavailable();
                return;
            }
            timers.startSingleTimer(
                    "hydrate-retry", new RetryHydration(incarnation, token.get()), Duration.ofMillis(25));
        } else if (!ready) warmHydration(true);
        else if (renewalDue) renewParticipants();
        else start();
    }

    private void reconcileRenewal(AuthoritySql.GroupToken token) {
        renewalLastStart = System.nanoTime();
        renewalNextDue = renewalLastStart + renewalPeriod().toNanos();
        var repair = Objects.requireNonNull(renewalRepair);
        pending = new Pending(null, token, Purpose.RECONCILE_RENEW);
        submit(() -> repair.reconcile(snapshot.get(), token, backend, Duration.ofSeconds(2)));
    }

    private void retryRenewalAfterSettlement() {
        retryHydration = false;
        // Four early retries per cadence, then use the normal phase interval. Each retry has one
        // bounded physical scope; unresolved physical owners never reach this method.
        Duration delay =
                ++renewalRetries <= 4
                        ? Duration.ofSeconds(1)
                        : snapshot.isPresent() ? renewalPeriod() : Duration.ofSeconds(5);
        if (renewalRetries > 4) renewalRetries = 0;
        renewalNextDue = System.nanoTime() + delay.toNanos();
        renewalDue = false;
        scheduleRenewal();
    }

    /**
     * Retry known warm-read overload only after cleanup, within the original budget and bounded
     * attempts.
     */
    private void warmHydration(boolean original) {
        if (original) {
            hydrationEnd = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            hydrationAttempts = 0;
        }
        var token = current();
        long remaining = hydrationEnd - System.nanoTime();
        if (token.isEmpty() || remaining <= 0 || hydrationAttempts >= 16) {
            unknown = true;
            finishUnavailable();
            if (token.isPresent() && renewalRepair != null && !stopping) retryRenewalAfterSettlement();
            else renewalRepair = null;
            return;
        }
        hydrationAttempts++;
        pending =
                new Pending(
                        null, token.get(), renewalRepair == null ? Purpose.HYDRATE : Purpose.RECONCILE_HYDRATE);
        submit(() -> backend.load(call, pending.token, Duration.ofNanos(remaining)));
    }

    private Behavior<CallMessage> retryHydration(RetryHydration retry) {
        if (!retry.actor().equals(incarnation)
                || pending != null
                || ready
                || unknown && renewalRepair == null
                || stopping) return this;
        if (current().filter(retry.token()::equals).isEmpty()) {
            unknown = true;
            renewalRepair = null;
            finishUnavailable();
            return next();
        }
        warmHydration(false);
        return next();
    }

    private void finishUnavailable() {
        while (!queue.isEmpty()) {
            var r = queue.remove();
            bytes -= size(r);
            reject(r, "UNAVAILABLE");
        }
    }

    private Behavior<CallMessage> expire(Expire e) {
        if (!incarnation.equals(e.actor())
                || unknown
                || stopping
                || snapshot.isEmpty()
                || snapshot.get().version() != e.version()
                || !current().filter(e.token()::equals).isPresent()) return this;
        if (pending != null || !queue.isEmpty()) {
            timers.startSingleTimer("deadline", e, Duration.ofMillis(50));
            return this;
        }
        pending = new Pending(null, e.token(), Purpose.EXPIRE);
        submit(() -> backend.expire(call, e.token(), e.version(), Duration.ofSeconds(2)));
        return this;
    }

    private Duration renewalPeriod() {
        return Duration.ofSeconds(snapshot.get().state().equals("PREPARING") ? 5 : 10);
    }

    private void scheduleRenewal() {
        timers.startSingleTimer(
                Renew.INSTANCE, Duration.ofNanos(Math.max(1, renewalNextDue - System.nanoTime())));
    }

    private Behavior<CallMessage> renewParticipants() {
        if (renewalRepair != null && unknown && !stopping && !passivating && pending == null) {
            if (System.nanoTime() - renewalNextDue < 0) {
                scheduleRenewal();
                return this;
            }
            ready = false;
            warmHydration(true);
            return next();
        }
        if (renewal == null
                || unknown
                || stopping
                || passivating
                || !ready
                || snapshot.isEmpty()
                || snapshot.get().terminalAt() != null) {
            renewalDue = false;
            if (ready && !unknown && !stopping && !passivating) start();
            return this;
        }
        if (renewalStarted && System.nanoTime() - renewalNextDue < 0) {
            renewalDue = false;
            scheduleRenewal();
            if (pending == null) start();
            return this;
        }
        renewalDue = true;
        if (pending != null) return this;
        renewalDue = false;
        var token = current();
        if (token.isEmpty()) return this;
        renewalStarted = true;
        renewalLastStart = System.nanoTime();
        renewalNextDue = renewalLastStart + renewalPeriod().toNanos();
        scheduleRenewal();
        pending = new Pending(null, token.get(), Purpose.RENEW);
        submit(() -> renewal.renew(snapshot.get(), token.get(), backend, Duration.ofSeconds(2)));
        return this;
    }

    /**
     * Relay frames bypass the command queue: they mutate no call state, and the relay producer keeps
     * its own per-call order. Only the group owner runs them; every outcome signals the receipt.
     */
    private Behavior<CallMessage> relay(Relay r) {
        long left = Duration.between(clock.instant(), r.deadline()).toNanos();
        if (!call.equals(r.call())
                || current().isEmpty()
                || unknown
                || stopping
                || passivating
                || left <= 0) {
            r.replyTo().tell(new RelayReply(null, left <= 0 ? "RELAY_EXPIRED" : "NOT_OWNER"));
            if (r.receipt() != null) r.receipt().signal();
            return this;
        }
        final io.webrtc.signaling.actors.admission.ActorOperation<String> operation;
        try {
            operation =
                    Objects.requireNonNull(
                            relayHandler.relay(r, Duration.ofNanos(Math.min(left, Duration.ofSeconds(2).toNanos()))));
        } catch (RuntimeException failed) {
            r.replyTo().tell(new RelayReply(null, "RELAY_FAILED"));
            if (r.receipt() != null) r.receipt().signal();
            return this;
        }
        operation
                .logical()
                .whenComplete(
                        (reply, failure) ->
                                r.replyTo()
                                        .tell(
                                                failure == null && reply != null
                                                        ? new RelayReply(reply, null)
                                                        : new RelayReply(null, "RELAY_FAILED")));
        operation
                .physicalCompletion()
                .whenComplete(
                        (done, failure) -> {
                            if (r.receipt() != null) r.receipt().signal();
                        });
        return this;
    }

    private Behavior<CallMessage> observe(GetSnapshot r) {
        if (ready || unknown) r.replyTo().tell(snapshot);
        else if (observers.size() < 16) observers.add(r.replyTo());
        else r.replyTo().tell(Optional.empty());
        return this;
    }

    private Behavior<CallMessage> retire() {
        if (pending != null || !queue.isEmpty()) {
            timers.startSingleTimer(Retire.INSTANCE, Duration.ofSeconds(1));
            return this;
        }
        if (shard == null) return Behaviors.stopped();
        passivating = true;
        shard.tell(new ClusterSharding.Passivate<>(getContext().getSelf()));
        return this;
    }

    private Behavior<CallMessage> stop() {
        invalidateRelay.accept(call);
        timers.cancel("hydrate-retry");
        timers.cancel(Renew.INSTANCE);
        retryHydration = false;
        renewalRepair = null;
        stopping = true;
        finishUnavailable();
        return next();
    }

    private Behavior<CallMessage> next() {
        if (pending == null && stopping) return Behaviors.stopped();
        if (pending == null && unknown && renewalRepair == null)
            return shard == null ? Behaviors.stopped() : retire();
        return this;
    }

    private void reject(CallMessage request, String code) {
        var receipt = receipt(request);
        if (receipt != null) {
            if (request instanceof WakeCall w) {
                try {
                    RecoveryWakeRegistry.get(getContext().getSystem())
                            .reserve(w.call(), current().orElse(null), receipt)
                            .notStarted();
                } catch (RecoveryWakeRegistry.AlreadyRegistered original) {
                    original
                            .entry()
                            .physical()
                            .whenComplete(
                                    (v, failure) -> {
                                        if (failure == null) receipt.signal();
                                    });
                    reply(request, code);
                    return;
                } catch (DbOverloadedException full) {
                    // The original request was observed and rejected before backend submission.
                }
            }
            receipt.signal();
        }
        reply(request, code);
    }

    private void reply(CallMessage r, String code) {
        switch (r) {
            case GrantToHome g -> g.replyTo().tell(new GrantReply(code, null));
            case Progress p ->
                    p.replyTo().tell(new CallWorkflowService.Outcome(code, snapshot.orElse(null), List.of()));
            case Execute e -> e.replyTo()
                    .tell(
                            new CallCommandService.Outcome(
                                    code,
                                    code,
                                    call,
                                    snapshot.map(Snapshot::version).orElse(0L),
                                    snapshot.map(Snapshot::state).orElse("NONE"),
                                    List.of()));
            case WakeCall w -> w.replyTo().tell(false);
            default -> {
            }
        }
    }

    public enum Stop implements CallMessage {
        INSTANCE
    }

    private enum Renew implements CallMessage {
        INSTANCE
    }

    private enum Retire implements CallMessage {
        INSTANCE
    }

    private enum Purpose {
        HYDRATE,
        WORK,
        EXPIRE,
        RENEW,
        RECONCILE_HYDRATE,
        RECONCILE_RENEW
    }

    public interface Backend {
        default HomeParticipationService.Request sealGrant(
                HomeParticipationService.Request request,
                String destination,
                CoordinatorGrantService.Issued issued) {
            throw new IllegalStateException("Native coordinator proof signer is required");
        }

        default DbOperation<CoordinatorGrantService.Issued> grant(
                HomeParticipationService.Request r,
                HomeParticipationService.AuthorizationIntent action,
                AuthoritySql.GroupToken token,
                long version,
                Duration budget) {
            throw new IllegalStateException("Coordinator grant service is required");
        }

        default DbOperation<CoordinatorGrantService.Issued> renewalProjectionGrant(
                HomeParticipationService.Request request,
                AuthoritySql.GroupToken token,
                long version,
                Duration budget) {
            throw new IllegalStateException("Protected native renewal projection grants are required");
        }

        default DbOperation<Optional<Snapshot>> loadCold(
                CallId call, AuthoritySql.GroupToken token, Duration budget) {
            return load(call, token, budget);
        }

        DbOperation<Optional<Snapshot>> load(
                CallId call, AuthoritySql.GroupToken token, Duration budget);

        DbOperation<CallWorkflowService.Outcome> progress(
                CallWorkflowService.Transition transition, Duration budget);

        DbOperation<CallCommandService.Outcome> command(
                CallCommand command,
                AuthoritySql.GroupToken token,
                long version,
                String proof,
                Duration budget);

        DbOperation<CallWorkflowService.Outcome> expire(
                CallId call, AuthoritySql.GroupToken token, long version, Duration budget);
    }

    public record GrantReply(
            String code,
            CoordinatorGrantService.Issued issued,
            HomeParticipationService.Request signedRequest)
            implements ApplicationSerializable {
        public GrantReply(String code, CoordinatorGrantService.Issued issued) {
            this(code, issued, null);
        }
    }

    public record GrantToHome(
            HomeParticipationService.Request request,
            HomeParticipationService.AuthorizationIntent action,
            String destination,
            ActorRef<GrantReply> replyTo,
            Instant deadline,
            int encodedBytes,
            io.webrtc.signaling.actors.admission.CompletionReceipt receipt,
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            io.webrtc.signaling.observability.TraceContext trace)
            implements CallMessage {
        public GrantToHome(
                HomeParticipationService.Request request,
                HomeParticipationService.AuthorizationIntent action,
                String destination,
                ActorRef<GrantReply> replyTo,
                Instant deadline,
                int encodedBytes,
                io.webrtc.signaling.actors.admission.CompletionReceipt receipt) {
            this(request, action, destination, replyTo, deadline, encodedBytes, receipt, null);
        }

        public GrantToHome(
                HomeParticipationService.Request request,
                HomeParticipationService.AuthorizationIntent action,
                String destination,
                ActorRef<GrantReply> replyTo,
                Instant deadline,
                int encodedBytes) {
            this(request, action, destination, replyTo, deadline, encodedBytes, null);
        }

        public GrantToHome(
                HomeParticipationService.Request request,
                HomeParticipationService.AuthorizationIntent action,
                ActorRef<GrantReply> replyTo,
                Instant deadline,
                int encodedBytes) {
            this(request, action, request.call().coordinatorCell(), replyTo, deadline, encodedBytes);
        }

        public GrantToHome {
            if (destination == null || !destination.matches("[a-z][a-z0-9-]{0,23}"))
                throw new IllegalArgumentException("Invalid destination");
            Objects.requireNonNull(request);
            Objects.requireNonNull(action);
            Objects.requireNonNull(replyTo);
            check(deadline, encodedBytes);
        }
    }

    public record Progress(
            CallWorkflowService.Transition transition,
            ActorRef<CallWorkflowService.Outcome> replyTo,
            Instant deadline,
            int encodedBytes,
            io.webrtc.signaling.actors.admission.CompletionReceipt receipt,
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            io.webrtc.signaling.observability.TraceContext trace)
            implements CallMessage {
        public Progress(
                CallWorkflowService.Transition transition,
                ActorRef<CallWorkflowService.Outcome> replyTo,
                Instant deadline,
                int encodedBytes,
                io.webrtc.signaling.actors.admission.CompletionReceipt receipt) {
            this(transition, replyTo, deadline, encodedBytes, receipt, null);
        }

        public Progress(
                CallWorkflowService.Transition transition,
                ActorRef<CallWorkflowService.Outcome> replyTo,
                Instant deadline,
                int encodedBytes) {
            this(transition, replyTo, deadline, encodedBytes, null);
        }

        public Progress {
            Objects.requireNonNull(transition);
            Objects.requireNonNull(replyTo);
            check(deadline, encodedBytes);
        }
    }

    public record Execute(
            CallCommand command,
            String proof,
            ActorRef<CallCommandService.Outcome> replyTo,
            Instant deadline,
            int encodedBytes,
            io.webrtc.signaling.actors.admission.CompletionReceipt receipt,
            @com.fasterxml.jackson.annotation.JsonInclude(
                    com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
            io.webrtc.signaling.observability.TraceContext trace)
            implements CallMessage {
        public Execute(
                CallCommand command,
                String proof,
                ActorRef<CallCommandService.Outcome> replyTo,
                Instant deadline,
                int encodedBytes) {
            this(command, proof, replyTo, deadline, encodedBytes, null, null);
        }

        public Execute(
                CallCommand command,
                String proof,
                ActorRef<CallCommandService.Outcome> replyTo,
                Instant deadline,
                int encodedBytes,
                io.webrtc.signaling.actors.admission.CompletionReceipt receipt) {
            this(command, proof, replyTo, deadline, encodedBytes, receipt, null);
        }

        public Execute {
            Objects.requireNonNull(command);
            Objects.requireNonNull(replyTo);
            check(deadline, encodedBytes);
        }
    }

    /** Runs one relay frame on the owning node; the logical value is the encoded reply. */
    @FunctionalInterface
    public interface RelayHandler {
        RelayHandler UNAVAILABLE =
                (relay, budget) ->
                        new io.webrtc.signaling.actors.admission.ActorOperation<>(
                                CompletableFuture.failedFuture(new IllegalStateException("Relay unavailable")),
                                CompletableFuture.completedFuture(null));

        io.webrtc.signaling.actors.admission.ActorOperation<String> relay(Relay relay, Duration budget);
    }

    /**
     * A relay frame validated at the receiving actor, forwarded to the call's owner. {@code command}
     * is the base64 internal command; the peer is the authenticated sender of the original RPC.
     */
    public record Relay(
            CallId call,
            String command,
            String peerCell,
            String peerRole,
            String peerWorkload,
            ActorRef<RelayReply> replyTo,
            Instant deadline,
            int encodedBytes,
            io.webrtc.signaling.actors.admission.CompletionReceipt receipt)
            implements CallMessage {
        public Relay {
            Objects.requireNonNull(call);
            Objects.requireNonNull(command);
            Objects.requireNonNull(peerCell);
            Objects.requireNonNull(peerRole);
            Objects.requireNonNull(peerWorkload);
            Objects.requireNonNull(replyTo);
            Objects.requireNonNull(deadline);
            if (command.length() > 131072) throw new IllegalArgumentException("Relay frame too large");
        }
    }

    /** Exactly one of {@code reply} (base64 internal reply) or {@code error} is set. */
    public record RelayReply(String reply, String error) implements io.webrtc.signaling.protocol.ApplicationSerializable {
        public RelayReply {
            if ((reply == null) == (error == null))
                throw new IllegalArgumentException("Relay reply needs exactly one outcome");
        }
    }

    public record WakeCall(
            CallId call,
            ActorRef<Boolean> replyTo,
            Instant deadline,
            int encodedBytes,
            io.webrtc.signaling.actors.admission.CompletionReceipt receipt)
            implements CallMessage {
        public WakeCall(CallId call, ActorRef<Boolean> replyTo, Instant deadline) {
            this(call, replyTo, deadline, 128, null);
        }

        public WakeCall {
            Objects.requireNonNull(call);
            Objects.requireNonNull(replyTo);
            check(deadline, encodedBytes);
        }
    }

    public record ReconcileWake(
            CallId call,
            io.webrtc.signaling.actors.admission.CompletionReceipt original,
            Instant deadline)
            implements CallMessage {
        public ReconcileWake {
            Objects.requireNonNull(call);
            Objects.requireNonNull(original);
            Objects.requireNonNull(deadline);
        }
    }

    public record GetSnapshot(ActorRef<Optional<Snapshot>> replyTo) implements CallMessage {
    }

    private record Completed(
            UUID actor, UUID operation, AuthoritySql.GroupToken token, Object value, Throwable error)
            implements CallMessage {
    }

    private record Cleaned(UUID actor, UUID operation, Throwable error) implements CallMessage {
    }

    private record Expire(UUID actor, AuthoritySql.GroupToken token, long version)
            implements CallMessage {
    }

    private record RetryHydration(UUID actor, AuthoritySql.GroupToken token) implements CallMessage {
    }

    private static final class Pending {
        final UUID id = UUID.randomUUID();
        final CallMessage request;
        final AuthoritySql.GroupToken token;
        final Purpose purpose;
        boolean logical, physical;
        Throwable failure;

        Pending(CallMessage request, AuthoritySql.GroupToken token, Purpose purpose) {
            this.request = request;
            this.token = token;
            this.purpose = purpose;
        }

        boolean hydration() {
            return purpose == Purpose.HYDRATE || purpose == Purpose.RECONCILE_HYDRATE;
        }

        boolean renewal() {
            return purpose == Purpose.RENEW || purpose == Purpose.RECONCILE_RENEW;
        }
    }
}
