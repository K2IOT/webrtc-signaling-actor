package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.observability.*;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.protocol.RuntimeCapabilities;
import io.webrtc.signaling.protocol.internal.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gateway process ingress: original volatile write receipts, authenticated coordinator, boot-bound
 * target.
 */
public final class GatewayRelayRpcServer implements AutoCloseable {
  @FunctionalInterface
  public interface Backend {
    RpcOperation<RelayWriteReceipt> send(
        RelayDelivery delivery, CellRpcServer.Peer peer, Duration budget);
  }

  @FunctionalInterface
  public interface ControlBackend {
    RpcOperation<ControlApplicationReceipt> send(
        ControlEvent event,
        io.webrtc.signaling.storage.SessionRepository.Route route,
        CellRpcServer.Peer peer,
        Duration budget);
  }

  private ControlBackend control;
  private final ExecutorService controlCpu =
      new ThreadPoolExecutor(
          2,
          2,
          0,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(128),
          Thread.ofPlatform().daemon().name("gateway-control-rpc-", 0).factory(),
          new ThreadPoolExecutor.AbortPolicy());

  public synchronized GatewayRelayRpcServer control(ControlBackend backend) {
    if (server != null || control != null)
      throw new IllegalStateException("Control backend installation");
    control = Objects.requireNonNull(backend);
    return this;
  }

  private static final Context.Key<CellRpcServer.Peer> PEER =
      Context.key("gateway-relay-authenticated-peer");
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(16)
                          .maxStringLength(81920)
                          .build())
                  .build())
          .findAndRegisterModules()
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final String cell, gateway, environment;
  private final UUID boot;
  private final int requestedPort;
  private final SslContext tls;
  private final RpcAdmission admission;
  private final Backend backend;
  private final NativeRpcDispatch preCall =
      new NativeRpcDispatch(
          "gateway-rpc-dispatch",
          Set.of(GatewayRelayIngressGrpc.getDeliverRelayMethod().getFullMethodName()),
          List.of(
              GatewayRelayIngressGrpc.getServiceDescriptor(),
              GatewayControlIngressGrpc.getServiceDescriptor(),
              CompatibilityObservationIngressGrpc.getServiceDescriptor()));
  private final ExecutorService cpu =
      new ThreadPoolExecutor(
          4,
          4,
          0,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(256),
          Thread.ofPlatform().daemon().name("gateway-relay-rpc-", 0).factory(),
          new ThreadPoolExecutor.AbortPolicy());
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private Server server;
  private volatile boolean draining;
  private volatile RuntimeCapabilities capabilities = RuntimeCapabilities.DENY;
  private volatile java.util.function.Supplier<CompatibilityObservationReply> compatibilityFacts = () -> null;

  public GatewayRelayRpcServer compatibilityFacts(java.util.function.Supplier<CompatibilityObservationReply> source) {
    Objects.requireNonNull(source);
    compatibilityFacts = () -> {
      var facts = source.get();
      return facts == null ? null : facts.toBuilder().setSourceCell(cell).setGatewayId(gateway).setBootId(boot.toString()).build();
    };
    return this;
  }

  private volatile OperationObservation observations = OperationObservation.NONE;

  public CompatibilityObservationReply localCompatibilityFacts() { return compatibilityFacts.get(); }

  public GatewayRelayRpcServer compatibility(RuntimeCapabilities capabilities) {
    this.capabilities = Objects.requireNonNull(capabilities);
    return this;
  }

  public GatewayRelayRpcServer observe(OperationObservation observations) {
    this.observations = Objects.requireNonNull(observations);
    return this;
  }

  public GatewayRelayRpcServer(
      String cell,
      String gatewayId,
      UUID bootId,
      String environment,
      int port,
      SslContext tls,
      RpcAdmission admission,
      Backend backend) {
    if (cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || gatewayId == null
        || !gatewayId.matches("[A-Za-z0-9_.-]{1,128}")
        || bootId == null
        || environment == null
        || !environment.matches("[a-z0-9-]{1,32}")
        || port < 0
        || port > 65535) throw new IllegalArgumentException("Invalid gateway RPC identity");
    this.cell = cell;
    gateway = gatewayId;
    boot = bootId;
    this.environment = environment;
    requestedPort = port;
    this.tls = Objects.requireNonNull(tls);
    this.admission = Objects.requireNonNull(admission);
    this.backend = Objects.requireNonNull(backend);
  }

  public synchronized GatewayRelayRpcServer start() throws java.io.IOException {
    if (draining || server != null) throw new IllegalStateException("Gateway RPC unavailable");
    server =
        NettyServerBuilder.forPort(requestedPort)
            .sslContext(tls)
            .maxInboundMessageSize(98304)
            .maxConcurrentCallsPerConnection(128)
            .executor(preCall)
            .addStreamTracerFactory(preCall.tracer())
            .callExecutor(
                new ServerCallExecutorSupplier() {
                  @Override
                  public <Q, A> Executor getExecutor(ServerCall<Q, A> call, Metadata headers) {
                    return call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals(GatewayRelayIngressGrpc.getDeliverRelayMethod().getFullMethodName())
                        ? cpu
                        : controlCpu;
                  }
                })
            .addService(new CompatibilityObservationService(cell, gateway, boot.toString(), admission, PEER::get, () -> compatibilityFacts.get()))
            .intercept(NativeRpcTelemetry.server())
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <Q, A> ServerCall.Listener<Q> interceptCall(
                      ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
                    var peer =
                        RpcTlsIdentity.extract(
                            call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION), environment);
                    if (peer == null
                        || !peer.role().equals("actor")
                        || peer.workloadId().isBlank()) {
                      call.close(
                          Status.PERMISSION_DENIED.withDescription("UNAUTHORIZED"), new Metadata());
                      return new ServerCall.Listener<>() {};
                    }
                    return Contexts.interceptCall(
                        Context.current().withValue(PEER, peer), call, headers, next);
                  }
                })
            .addService(
                new GatewayRelayIngressGrpc.GatewayRelayIngressImplBase() {
                  @Override
                  public void deliverRelay(
                      GatewayRelayRequest request, StreamObserver<InternalReply> reply) {
                    deliver(request, reply);
                  }
                })
            .addService(
                new GatewayControlIngressGrpc.GatewayControlIngressImplBase() {
                  @Override
                  public void deliverControl(
                      GatewayControlRequest request, StreamObserver<InternalReply> reply) {
                    controlDelivery(request, reply);
                  }
                })
            .build()
            .start();
    return this;
  }

  public synchronized int port() {
    if (server == null) throw new IllegalStateException("Gateway RPC not started");
    return server.getPort();
  }

  private void deliver(GatewayRelayRequest request, StreamObserver<InternalReply> reply) {
    final long end;
    final RpcAdmission.Ticket ticket;
    try {
      var deadline = Context.current().getDeadline();
      capabilities.requireGateway(cell, gateway, boot, RuntimeCapabilities.FULL_CONNECTION_ACTIVE);
      var source = PEER.get();
      if (source == null) throw new IllegalStateException("Unknown relay peer");
      capabilities.require(source.cell(), RuntimeCapabilities.FULL_CONNECTION_ACTIVE);
      if (draining
          || request.getSchemaMajor() != 1
          || !cell.equals(request.getDestinationCell())
          || !gateway.equals(request.getGatewayId())
          || !boot.equals(RpcBusinessHandler.uuid(request.getBootId()))
          || request.getSerializedSize() > 98304
          || request.getPayload().isEmpty()
          || request.getPayload().size() > 81920
          || request.getRemainingBudgetMs() <= 0
          || deadline == null) throw new IllegalArgumentException();
      UUID.fromString(request.getOperationId());
      long remaining =
          Math.min(
              1000,
              Math.min(
                  request.getRemainingBudgetMs(), deadline.timeRemaining(TimeUnit.MILLISECONDS)));
      if (remaining <= 0) throw new IllegalArgumentException();
      end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(remaining);
      ticket = admission.acquire(RpcAdmission.Lane.RELAY, request.getSerializedSize());
    } catch (RpcAdmission.Overloaded full) {
      respond(reply, error(request, "OVERLOADED"));
      return;
    } catch (RuntimeException invalid) {
      respond(reply, error(request, "INVALID_MESSAGE"));
      return;
    }
    final RelayDelivery delivery;
    try {
      delivery = JSON.readValue(request.getPayload().toByteArray(), RelayDelivery.class);
      if (!request.getOperationId().equals(delivery.command().requestId().value().toString())
          || !PEER.get().cell().equals(delivery.command().callId().coordinatorCell()))
        throw new IllegalArgumentException();
    } catch (Exception invalid) {
      ticket.close();
      respond(reply, error(request, "INVALID_MESSAGE"));
      return;
    }
    try {
      long remaining = end - System.nanoTime();
      if (remaining <= 0) {
        ticket.close();
        respond(reply, error(request, "RESYNC_REQUIRED"));
        return;
      }
      var observation =
          OperationObservation.safeStart(observations, SignalingMetrics.Operation.RPC_RELAY);
      var operation =
          NativeRpcTelemetry.observe(
              backend.send(delivery, PEER.get(), Duration.ofNanos(remaining)),
              observation,
              value ->
                  value != null
                          && value.matches(delivery.command())
                          && value.callVersion() == delivery.callVersion()
                      ? SignalingMetrics.Outcome.READ
                      : SignalingMetrics.Outcome.OUTCOME_UNKNOWN);
      var replied = new CompletableFuture<Void>();
      operation.physicalCompletion().thenCombine(replied, (a, b) -> null).thenRun(ticket::close);
      operation
          .logical()
          .thenApply(value -> value)
          .toCompletableFuture()
          .orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS)
          .whenComplete(
              (receipt, failure) -> {
                try {
                  if (failure != null
                      || receipt == null
                      || !receipt.matches(delivery.command())
                      || receipt.callVersion() != delivery.callVersion())
                    respond(reply, error(request, "RESYNC_REQUIRED"));
                  else
                    respond(
                        reply,
                        InternalReply.newBuilder()
                            .setOperationId(request.getOperationId())
                            .setCallId(receipt.call().value())
                            .setCallVersion(receipt.callVersion())
                            .setStatus("WRITE_COMPLETED")
                            .setResult(
                                com.google.protobuf.ByteString.copyFrom(
                                    RpcBusinessHandler.encode(receipt)))
                            .build());
                } finally {
                  replied.complete(null);
                }
              });
    } catch (Throwable unknown) {
      // A throwing producer cannot prove it never started a native write. Retain this admission.
      respond(reply, error(request, "RESYNC_REQUIRED"));
    }
  }

  private void controlDelivery(GatewayControlRequest request, StreamObserver<InternalReply> reply) {
    var event = request.getEvent();
    var error =
        InternalReply.newBuilder()
            .setOperationId(event.getEventId())
            .setCallId(event.getCallId())
            .setCallVersion(event.getCallVersion())
            .setStatus("REJECTED")
            .setErrorCode("OUTCOME_UNKNOWN")
            .build();
    final RpcAdmission.Ticket ticket;
    final long end;
    final io.webrtc.signaling.storage.SessionRepository.Route route;
    final CellRpcServer.Peer peer = PEER.get();
    try {
      var deadline = Context.current().getDeadline();
      capabilities.requireGateway(
          cell, gateway, boot, RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1);
      if (peer == null) throw new IllegalStateException("Unknown control peer");
      capabilities.require(peer.cell(), RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1);
      var call = new io.webrtc.signaling.protocol.Identity.CallId(event.getCallId());
      UUID.fromString(event.getEventId());
      if (draining
          || control == null
          || deadline == null
          || request.getSchemaMajor() != 1
          || !cell.equals(request.getDestinationCell())
          || !gateway.equals(request.getGatewayId())
          || !boot.equals(RpcBusinessHandler.uuid(request.getBootId()))
          || request.getSerializedSize() > 16384
          || request.getRoute().size() > 4096
          || event.getMetadata().size() > 8192
          || event.getCallVersion() < 1
          || !event.getType().matches("[A-Z_]{1,64}")
          || peer == null
          || !peer.role().equals("actor")
          || peer.workloadId().isBlank()
          || !peer.cell().equals(call.coordinatorCell())
          || request.getRemainingBudgetMs() <= 0)
        throw new IllegalArgumentException("UNAUTHORIZED");
      long remaining =
          Math.min(
              2000,
              Math.min(
                  request.getRemainingBudgetMs(), deadline.timeRemaining(TimeUnit.MILLISECONDS)));
      if (remaining <= 0) throw new IllegalArgumentException("Original control deadline");
      end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(remaining);
      route =
          JSON.readValue(
              request.getRoute().toByteArray(),
              io.webrtc.signaling.storage.SessionRepository.Route.class);
      var destination = event.getDestination();
      if (!route.gatewayId().equals(gateway)
          || !route.bootId().equals(boot)
          || !route.user().value().equals(destination.getUserId())
          || !route.key().issuer().equals(destination.getIssuer())
          || !route.key().jti().equals(destination.getJti())
          || !route
              .incarnation()
              .value()
              .equals(RpcBusinessHandler.uuid(destination.getIncarnation()))
          || !route.connectionId().equals(RpcBusinessHandler.uuid(destination.getConnectionId()))
          || route.connectionGeneration() != destination.getConnectionGeneration())
        throw new IllegalArgumentException("STALE_BINDING");
      ticket = admission.acquire(RpcAdmission.Lane.CONTROL, request.getSerializedSize());
    } catch (Exception invalid) {
      respond(reply, error);
      return;
    }
    try {
      var observation =
          OperationObservation.safeStart(observations, SignalingMetrics.Operation.RPC_CONTROL);
      var operation =
          NativeRpcTelemetry.observe(
              control.send(
                  event, route, peer, Duration.ofNanos(Math.max(1, end - System.nanoTime()))),
              observation,
              value ->
                  value != null
                          && value.eventId().toString().equals(event.getEventId())
                          && value.call().value().equals(event.getCallId())
                          && value.callVersion() == event.getCallVersion()
                          && value.route().equals(route)
                      ? SignalingMetrics.Outcome.READ
                      : SignalingMetrics.Outcome.OUTCOME_UNKNOWN);
      var replied = new CompletableFuture<Void>();
      operation.physicalCompletion().thenCombine(replied, (a, b) -> null).thenRun(ticket::close);
      operation
          .logical()
          .toCompletableFuture()
          .copy()
          .orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS)
          .whenComplete(
              (receipt, failure) -> {
                try {
                  if (failure != null
                      || receipt == null
                      || !receipt.eventId().toString().equals(event.getEventId())
                      || !receipt.call().value().equals(event.getCallId())
                      || receipt.callVersion() != event.getCallVersion()
                      || !receipt.route().equals(route)) respond(reply, error);
                  else
                    respond(
                        reply,
                        InternalReply.newBuilder()
                            .setOperationId(event.getEventId())
                            .setCallId(event.getCallId())
                            .setCallVersion(event.getCallVersion())
                            .setStatus("APPLICATION_RECEIVED")
                            .setResult(
                                com.google.protobuf.ByteString.copyFrom(
                                    RpcBusinessHandler.encode(receipt)))
                            .build());
                } finally {
                  replied.complete(null);
                }
              });
    } catch (Throwable unknown) {
      // Producer failure cannot prove native dispatch was not started.
      respond(reply, error);
    }
  }

  private static InternalReply error(GatewayRelayRequest request, String code) {
    return InternalReply.newBuilder()
        .setOperationId(request.getOperationId())
        .setStatus("REJECTED")
        .setErrorCode(code)
        .build();
  }

  private static void respond(StreamObserver<InternalReply> observer, InternalReply reply) {
    observer.onNext(reply);
    observer.onCompleted();
  }

  public synchronized CompletionStage<Void> drain() {
    if (!draining) {
      draining = true;
      if (server != null) server.shutdown();
      admission
          .drain()
          .whenComplete(
              (v, failure) -> {
                if (failure != null) {
                  drained.completeExceptionally(failure);
                  return;
                }
                Thread.startVirtualThread(
                    () -> {
                      try {
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                        if (server != null && !server.awaitTermination(2, TimeUnit.SECONDS))
                          throw new TimeoutException("Gateway RPC transport cleanup unknown");
                        // Join lookup producers before closing their destination body pools.
                        preCall.shutdownAndAwait(end);
                        cpu.shutdown();
                        controlCpu.shutdown();
                        for (var executor : List.of(cpu, controlCpu)) {
                          long remaining = end - System.nanoTime();
                          if (remaining <= 0
                              || !executor.awaitTermination(remaining, TimeUnit.NANOSECONDS))
                            throw new TimeoutException("Gateway RPC CPU cleanup unknown");
                        }
                        drained.complete(null);
                      } catch (Exception unknown) {
                        if (unknown instanceof InterruptedException)
                          Thread.currentThread().interrupt();
                        drained.completeExceptionally(unknown);
                      }
                    });
              });
    }
    return drained.minimalCompletionStage();
  }

  public CompletionStage<Void> settleAdmitted() {
    return admission.settleAdmitted();
  }

  @Override
  public void close() {
    drain();
  }
}
