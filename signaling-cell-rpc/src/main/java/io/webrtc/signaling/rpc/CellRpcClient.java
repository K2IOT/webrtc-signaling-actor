package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.internal.*;
import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
/** Two lazy channels/destination, isolated flow control, one owner of bounded same-identity retry. */
public final class CellRpcClient implements AutoCloseable {
    public record Endpoint(String host,int port,String tlsAuthority){public Endpoint{if(host==null||host.isBlank()||port<1||port>65535||tlsAuthority==null||tlsAuthority.isBlank())throw new IllegalArgumentException("Invalid trusted RPC endpoint");}}
    private record ChannelKey(String cell,RpcAdmission.Lane lane) {}
    private final Map<String,Endpoint> destinations;private final RpcTlsContexts.ClientTls tls;private final String environment;private final RpcAdmission admission;private final ConcurrentHashMap<ChannelKey,ManagedChannel> channels=new ConcurrentHashMap<>();private volatile boolean closed;
    private final ExecutorService callbacks=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(256),Thread.ofPlatform().daemon().name("cell-rpc-client-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    /** Test-only raw context adapter; production supplies cell-bound handshake verification. */
    CellRpcClient(Map<String,Endpoint> destinations,SslContext tls,RpcAdmission admission){this("test",destinations,cell->tls,admission);}
    public CellRpcClient(String environment,Map<String,Endpoint> destinations,RpcTlsContexts.ClientTls tls,RpcAdmission admission){if(environment==null||!environment.matches("[a-z0-9-]{1,32}")||destinations.isEmpty()||destinations.size()>50||destinations.keySet().stream().anyMatch(c->!c.matches("[a-z][a-z0-9-]{0,23}")))throw new IllegalArgumentException("Invalid bounded destination topology");this.environment=environment;this.destinations=Map.copyOf(destinations);this.tls=Objects.requireNonNull(tls);this.admission=Objects.requireNonNull(admission);retryTimers.setRemoveOnCancelPolicy(true);}
    public int channelCount(){return channels.size();}
    private synchronized ManagedChannel channel(String cell,RpcAdmission.Lane lane){if(closed)throw new IllegalStateException("RPC client draining");Endpoint endpoint=destinations.get(cell);if(endpoint==null)throw new IllegalArgumentException("Unknown destination");return channels.computeIfAbsent(new ChannelKey(cell,lane),key->NettyChannelBuilder.forAddress(endpoint.host(),endpoint.port()).overrideAuthority(endpoint.tlsAuthority()).sslContext(tls.context(cell)).disableRetry().maxInboundMessageSize(98304).executor(callbacks).intercept(new ClientInterceptor(){@Override public <Q,A> ClientCall<Q,A> interceptCall(MethodDescriptor<Q,A> method,CallOptions options,Channel next){var delegate=next.newCall(method,options);return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate){@Override public void start(ClientCall.Listener<A> listener,Metadata headers){super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener){boolean rejected;@Override public void onHeaders(Metadata metadata){var peer=RpcTlsIdentity.extract(delegate.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION),environment);if(peer==null||!peer.cell().equals(cell)||!peer.role().equals("actor")){rejected=true;delegate.cancel("UNAUTHORIZED",null);}else super.onHeaders(metadata);}@Override public void onMessage(A message){if(!rejected)super.onMessage(message);}@Override public void onClose(Status status,Metadata trailers){super.onClose(rejected?Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED"):status,trailers);}},headers);}};}}).build());}
    private final ScheduledThreadPoolExecutor retryTimers=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon().name("cell-rpc-retry").factory());
    private final Set<Flight<?>> active = new HashSet<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>(), settled = new CompletableFuture<>();
    private final class Flight<T> {
        final RpcAdmission.Ticket ticket;
        final CompletableFuture<T> logical = new CompletableFuture<>();
        final CompletableFuture<Void> physical = new CompletableFuture<>();
        int streams = 1; // Dispatch sentinel: synchronously completed streams cannot retire unfinished dispatch.
        Flight(RpcAdmission.Ticket ticket) { this.ticket = ticket; }
        synchronized void opened() { streams++; }
        synchronized void ended() {
            if (--streams != 0) return;
            ticket.close();
            synchronized (CellRpcClient.this) {
                active.remove(this);
                physical.complete(null);
                if (closed && active.isEmpty()) settled.complete(null);
            }
        }
        RpcOperation<T> operation() { return new RpcOperation<>(logical.minimalCompletionStage(), physical.minimalCompletionStage()); }
    }
    private synchronized <T> Flight<T> begin(RpcAdmission.Lane lane, int bytes) {
        if (closed) return null;
        var flight = new Flight<T>(admission.acquire(lane, bytes)); active.add(flight); return flight;
    }
    private static <T> RpcOperation<T> completed(T value) {
        return new RpcOperation<>(CompletableFuture.completedFuture(value).minimalCompletionStage(), CompletableFuture.completedFuture(null).minimalCompletionStage());
    }
    public CompletionStage<InternalReply> call(CellRpcServer.Operation op, InternalCommand original, Duration budget) {
        return callTracked(op, original, budget).logical();
    }
    public RpcOperation<InternalReply> callTracked(CellRpcServer.Operation op, InternalCommand original, Duration budget) {
        if (closed || budget == null || budget.isNegative() || budget.isZero() || original.getRemainingBudgetMs() <= 0)
            return completed(error(original, "OUTCOME_UNKNOWN"));
        if (!destinations.containsKey(original.getDestinationCell())) return completed(error(original, "WRONG_CELL"));
        var lane = op == CellRpcServer.Operation.RELAY ? RpcAdmission.Lane.RELAY : RpcAdmission.Lane.CONTROL;
        long nanos = Math.min(budget.toNanos(), Math.min(Duration.ofSeconds(lane == RpcAdmission.Lane.RELAY ? 1 : 2).toNanos(), TimeUnit.MILLISECONDS.toNanos(original.getRemainingBudgetMs())));
        Flight<InternalReply> flight;
        try { flight = begin(lane, original.getSerializedSize()); }
        catch (RpcAdmission.Overloaded full) { return completed(error(original, "OVERLOADED")); }
        if (flight == null) return completed(error(original, "OUTCOME_UNKNOWN"));
        try { attempt(op, original, System.nanoTime() + nanos, 0, lane, flight); }
        catch (RuntimeException failed) { flight.logical.complete(error(original, "OUTCOME_UNKNOWN")); }
        finally { flight.ended(); }
        return flight.operation();
    }
    private void attempt(CellRpcServer.Operation op, InternalCommand original, long end, int attempt, RpcAdmission.Lane lane, Flight<InternalReply> flight) {
        long remaining = end - System.nanoTime();
        if (closed || remaining <= 0) { flight.logical.complete(error(original, "OUTCOME_UNKNOWN")); return; }
        var command = original.toBuilder().setRemainingBudgetMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining))).build();
        var stub = CellIngressGrpc.newStub(channel(original.getDestinationCell(), lane)).withDeadlineAfter(remaining, TimeUnit.NANOSECONDS).withWaitForReady();
        flight.opened();
        var terminal = new java.util.concurrent.atomic.AtomicBoolean();
        var observer = new StreamObserver<InternalReply>() {
            boolean received;
            @Override public void onNext(InternalReply value) {
                received = true;
                flight.logical.complete(original.getOperationId().equals(value.getOperationId()) && original.getCallId().equals(value.getCallId()) ? value : error(original, "OUTCOME_UNKNOWN"));
            }
            @Override public void onCompleted() {
                if (!terminal.compareAndSet(false, true)) return;
                try { if (!received) flight.logical.complete(error(original, "OUTCOME_UNKNOWN")); } finally { flight.ended(); }
            }
            @Override public void onError(Throwable failure) {
                if (!terminal.compareAndSet(false, true)) return;
                try {
                    var code = Status.fromThrowable(failure).getCode();
                    var delay=!received&&code==Status.Code.UNAVAILABLE&&!closed
                        ?RpcRetryBackoff.delayNanos(attempt,ThreadLocalRandom.current().nextDouble(),end-System.nanoTime()):OptionalLong.empty();
                    if(delay.isPresent()){
                        // Retain the original ticket across the delayed dispatch as well as every transport stream.
                        flight.opened();
                        try{retryTimers.schedule(()->{
                            try{CellRpcClient.this.attempt(op,original,end,attempt+1,lane,flight);}
                            catch(RuntimeException rejected){flight.logical.complete(error(original,"OUTCOME_UNKNOWN"));}
                            finally{flight.ended();}
                        },delay.getAsLong(),TimeUnit.NANOSECONDS);}
                        catch(RejectedExecutionException notStarted){flight.logical.complete(error(original,"OUTCOME_UNKNOWN"));flight.ended();}
                    }else flight.logical.complete(error(original,code==Status.Code.PERMISSION_DENIED||code==Status.Code.UNAUTHENTICATED?"UNAUTHORIZED":"OUTCOME_UNKNOWN"));
                } finally { flight.ended(); }
            }
        };
        try {
            switch (op) {
                case RESERVE -> stub.reserveUser(command, observer); case CLAIM -> stub.claimAccept(command, observer);
                case RELEASE -> stub.releaseIfCallVersion(command, observer); case EXECUTE -> stub.executeCallCommand(command, observer);
                case RELAY -> stub.relayNegotiation(command, observer); case SYNC -> stub.syncCall(command, observer);
                default -> throw new IllegalArgumentException("Use event delivery method");
            }
        } catch (RuntimeException failed) { observer.onError(failed); }
    }
    public CompletionStage<InternalReply> deliver(String destination, ControlEvent event, Duration budget) {
        return deliverTracked(destination, event, budget).logical();
    }
    public RpcOperation<InternalReply> deliverTracked(String destination, ControlEvent event, Duration budget) {
        var unknown = InternalReply.newBuilder().setOperationId(event.getEventId()).setCallId(event.getCallId()).setErrorCode("OUTCOME_UNKNOWN").build();
        if (closed || !destinations.containsKey(destination) || budget == null || budget.isNegative() || budget.isZero()) return completed(unknown);
        Flight<InternalReply> flight;
        try { flight = begin(RpcAdmission.Lane.CONTROL, event.getSerializedSize()); }
        catch (RpcAdmission.Overloaded full) { return completed(unknown.toBuilder().setErrorCode("OVERLOADED").build()); }
        if (flight == null) return completed(unknown);
        try {
            var stub = CellIngressGrpc.newStub(channel(destination, RpcAdmission.Lane.CONTROL)).withDeadlineAfter(Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos()), TimeUnit.NANOSECONDS);
            flight.opened();
            var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer = new StreamObserver<InternalReply>() {
                @Override public void onNext(InternalReply value) { flight.logical.complete(event.getEventId().equals(value.getOperationId()) && event.getCallId().equals(value.getCallId()) ? value : unknown); }
                @Override public void onError(Throwable failure) { if (terminal.compareAndSet(false, true)) try { flight.logical.complete(unknown); } finally { flight.ended(); } }
                @Override public void onCompleted() { if (terminal.compareAndSet(false, true)) try { flight.logical.complete(unknown); } finally { flight.ended(); } }
            };
            try { stub.deliverControlEvent(event, observer); } catch (RuntimeException failed) { observer.onError(failed); }
        } catch (RuntimeException failed) { flight.logical.complete(unknown); }
        finally { flight.ended(); }
        return flight.operation();
    }
    public CompletionStage<SessionReply> session(SessionCommand command, Duration budget) { return sessionTracked(command, budget).logical(); }
    public RpcOperation<SessionReply> sessionTracked(SessionCommand command, Duration budget) {
        if (closed || budget == null || budget.isNegative() || budget.isZero() || command.getRemainingBudgetMs() <= 0 || !destinations.containsKey(command.getDestinationCell())) return completed(sessionError(command, "OUTCOME_UNKNOWN"));
        Flight<SessionReply> flight;
        try { flight = begin(RpcAdmission.Lane.CONTROL, command.getSerializedSize()); }
        catch (RpcAdmission.Overloaded full) { return completed(sessionError(command, "OVERLOADED")); }
        if (flight == null) return completed(sessionError(command, "OUTCOME_UNKNOWN"));
        try {
            long nanos = Math.min(Duration.ofSeconds(2).toNanos(), Math.min(budget.toNanos(), TimeUnit.MILLISECONDS.toNanos(command.getRemainingBudgetMs())));
            var stub = SessionIngressGrpc.newStub(channel(command.getDestinationCell(), RpcAdmission.Lane.CONTROL)).withDeadlineAfter(nanos, TimeUnit.NANOSECONDS);
            flight.opened(); var terminal = new java.util.concurrent.atomic.AtomicBoolean();
            var observer = new StreamObserver<SessionReply>() {
                @Override public void onNext(SessionReply value) { flight.logical.complete(command.getOperationId().equals(value.getOperationId()) ? value : sessionError(command, "OUTCOME_UNKNOWN")); }
                @Override public void onError(Throwable failure) { if (terminal.compareAndSet(false, true)) try { var code = Status.fromThrowable(failure).getCode(); flight.logical.complete(sessionError(command, code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED ? "UNAUTHORIZED" : "OUTCOME_UNKNOWN")); } finally { flight.ended(); } }
                @Override public void onCompleted() { if (terminal.compareAndSet(false, true)) try { flight.logical.complete(sessionError(command, "OUTCOME_UNKNOWN")); } finally { flight.ended(); } }
            };
            try { stub.mutateSession(command, observer); } catch (RuntimeException failed) { observer.onError(failed); }
        } catch (RuntimeException failed) { flight.logical.complete(sessionError(command, "OUTCOME_UNKNOWN")); }
        finally { flight.ended(); }
        return flight.operation();
    }
    /** Permanently stop new streams. Existing bounded streams keep their credit until terminal callbacks. */
    public synchronized CompletionStage<Void> settleAdmitted(){return CompletableFuture.allOf(active.stream().map(f->f.physical).toArray(CompletableFuture[]::new)).minimalCompletionStage();}
    public synchronized CompletionStage<Void> drain() {
        if (!closed) {
            closed = true;
            channels.values().forEach(ManagedChannel::shutdown);
            RpcTransportDrain.await(settled,List.copyOf(channels.values()),drained,callbacks,retryTimers);
            if (active.isEmpty()) settled.complete(null);
        }
        return drained.minimalCompletionStage();
    }
    private static SessionReply sessionError(SessionCommand command,String error){return SessionReply.newBuilder().setOperationId(command.getOperationId()).setStatus("REJECTED").setErrorCode(error).build();}
    private static InternalReply error(InternalCommand c,String code){return InternalReply.newBuilder().setOperationId(c.getOperationId()).setCallId(c.getCallId()).setStatus("REJECTED").setErrorCode(code).build();}
    @Override public void close(){drain();}
}
