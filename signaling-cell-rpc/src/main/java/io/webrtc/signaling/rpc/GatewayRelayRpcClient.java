package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.webrtc.signaling.protocol.internal.*;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Boot-enrolled destinations, two lazy physical relay channels, independent original stream
 * receipts.
 */
public final class GatewayRelayRpcClient implements AutoCloseable {
  public record Target(String cell, String gatewayId, UUID bootId) {
    public Target {
      if (cell == null
          || !cell.matches("[a-z][a-z0-9-]{0,23}")
          || gatewayId == null
          || !gatewayId.matches("[A-Za-z0-9_.-]{1,128}")
          || bootId == null) throw new IllegalArgumentException("Invalid gateway RPC target");
    }
  }

  private record ChannelKey(Target target, int stripe) {}

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
  private final String environment;
  private final Map<Target, CellRpcClient.Endpoint> destinations;
  private final RpcTlsContexts.GatewayClientTls tls;
  private final RpcAdmission admission;
  private final Map<ChannelKey, ManagedChannel> channels = new HashMap<>();
  private final Set<Flight> active = new HashSet<>();
  private final AtomicInteger stripe = new AtomicInteger();
  private final CompletableFuture<Void> drained = new CompletableFuture<>(),
      settled = new CompletableFuture<>();
  private boolean closed;
  private final ExecutorService callbacks =
      new ThreadPoolExecutor(
          4,
          4,
          0,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(256),
          Thread.ofPlatform().daemon().name("gateway-relay-client-", 0).factory(),
          new ThreadPoolExecutor.AbortPolicy());

  public GatewayRelayRpcClient(
      String environment,
      int maximumDestinations,
      Map<Target, CellRpcClient.Endpoint> destinations,
      RpcTlsContexts.GatewayClientTls tls,
      RpcAdmission admission) {
    if (environment == null
        || !environment.matches("[a-z0-9-]{1,32}")
        || maximumDestinations < 1
        || maximumDestinations > 16384
        || destinations.size() > maximumDestinations)
      throw new IllegalArgumentException("Invalid bounded gateway topology");
    this.environment = environment;
    this.destinations = Map.copyOf(destinations);
    this.tls = Objects.requireNonNull(tls);
    this.admission = Objects.requireNonNull(admission);
  }

  public synchronized int channelCount() {
    return channels.size();
  }

  private synchronized ManagedChannel channel(Target target) {
    if (closed) throw new IllegalStateException("Gateway relay client draining");
    var endpoint = destinations.get(target);
    if (endpoint == null) throw new IllegalArgumentException("Unenrolled gateway boot");
    var key = new ChannelKey(target, stripe.getAndIncrement() & 1);
    return channels.computeIfAbsent(
        key,
        k ->
            NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                .overrideAuthority(endpoint.tlsAuthority())
                .sslContext(tls.context(target.cell(), target.gatewayId()))
                .disableRetry()
                .maxInboundMessageSize(98304)
                .executor(callbacks)
                .intercept(
                    new ClientInterceptor() {
                      @Override
                      public <Q, A> ClientCall<Q, A> interceptCall(
                          MethodDescriptor<Q, A> method, CallOptions options, Channel next) {
                        var delegate = next.newCall(method, options);
                        return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate) {
                          @Override
                          public void start(ClientCall.Listener<A> listener, Metadata headers) {
                            super.start(
                                new ForwardingClientCallListener
                                    .SimpleForwardingClientCallListener<>(listener) {
                                  boolean rejected;

                                  @Override
                                  public void onHeaders(Metadata metadata) {
                                    var peer =
                                        RpcTlsIdentity.extract(
                                            delegate
                                                .getAttributes()
                                                .get(Grpc.TRANSPORT_ATTR_SSL_SESSION),
                                            environment);
                                    if (peer == null
                                        || !peer.cell().equals(target.cell())
                                        || !peer.role().equals("gateway")
                                        || !peer.workloadId().equals(target.gatewayId())) {
                                      rejected = true;
                                      delegate.cancel("UNAUTHORIZED", null);
                                    } else super.onHeaders(metadata);
                                  }

                                  @Override
                                  public void onMessage(A message) {
                                    if (!rejected) super.onMessage(message);
                                  }

                                  @Override
                                  public void onClose(Status status, Metadata trailers) {
                                    super.onClose(
                                        rejected
                                            ? Status.PERMISSION_DENIED.withDescription(
                                                "UNAUTHORIZED")
                                            : status,
                                        trailers);
                                  }
                                },
                                headers);
                          }
                        };
                      }
                    })
                .build());
  }

  private final class Flight {
    final RpcAdmission.Ticket ticket;
    final CompletableFuture<RelayWriteReceipt> logical = new CompletableFuture<>();
    final CompletableFuture<Void> physical = new CompletableFuture<>();
    int retained = 2;

    Flight(RpcAdmission.Ticket ticket) {
      this.ticket = ticket;
      logical.whenComplete((v, e) -> ended());
    }

    synchronized void opened() {
      retained++;
    }

    void ended() {
      boolean done;
      synchronized (this) {
        done = --retained == 0;
      }
      if (done) {
        ticket.close();
        synchronized (GatewayRelayRpcClient.this) {
          active.remove(this);
          physical.complete(null);
          if (closed && active.isEmpty()) settled.complete(null);
        }
      }
    }

    RpcOperation<RelayWriteReceipt> operation() {
      return new RpcOperation<>(
          logical.minimalCompletionStage(), physical.minimalCompletionStage());
    }
  }

  private synchronized Flight begin(int bytes) {
    if (closed) throw new IllegalStateException("Gateway relay client draining");
    var flight = new Flight(admission.acquire(RpcAdmission.Lane.RELAY, bytes));
    active.add(flight);
    return flight;
  }

  public RpcOperation<RelayWriteReceipt> send(
      RelayDestination destination, RelayDelivery delivery, Duration budget) {
    final Target target;
    final GatewayRelayRequest request;
    final long end;
    final Flight flight;
    try {
      Objects.requireNonNull(destination);
      Objects.requireNonNull(delivery);
      if (budget == null
          || budget.isNegative()
          || budget.isZero()
          || budget.compareTo(Duration.ofSeconds(1)) > 0
          || !destination.recipient().equals(delivery.recipient()))
        throw new IllegalArgumentException("Invalid relay budget/binding");
      end = System.nanoTime() + budget.toNanos();
      target = new Target(destination.cell(), destination.gatewayId(), destination.bootId());
      if (!destinations.containsKey(target))
        throw new IllegalArgumentException("Unenrolled gateway boot");
      var payload = JSON.writeValueAsBytes(delivery);
      if (payload.length > 81920)
        throw new IllegalArgumentException("Gateway relay body exceeds bound");
      request =
          GatewayRelayRequest.newBuilder()
              .setSchemaMajor(1)
              .setDestinationCell(target.cell())
              .setGatewayId(target.gatewayId())
              .setBootId(
                  ByteString.copyFrom(
                      ByteBuffer.allocate(16)
                          .putLong(target.bootId().getMostSignificantBits())
                          .putLong(target.bootId().getLeastSignificantBits())
                          .array()))
              .setOperationId(delivery.command().requestId().value().toString())
              .setPayload(ByteString.copyFrom(payload))
              .setRemainingBudgetMs(Math.max(1, budget.toMillis()))
              .build();
      if (request.getSerializedSize() > 98304 || end - System.nanoTime() <= 0)
        throw new IllegalArgumentException("Original relay deadline/bound");
      flight = begin(request.getSerializedSize());
    } catch (Exception invalid) {
      return new RpcOperation<>(
          CompletableFuture.failedFuture(invalid), CompletableFuture.completedFuture(null));
    }
    flight.logical.orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
    try {
      var transport = channel(target);
      long remaining = end - System.nanoTime();
      if (remaining <= 0) throw new TimeoutException("Original relay deadline");
      var stub =
          GatewayRelayIngressGrpc.newStub(transport)
              .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS);
      flight.opened();
      var terminal = new AtomicBoolean();
      var observer =
          new StreamObserver<InternalReply>() {
            @Override
            public void onNext(InternalReply reply) {
              try {
                if (reply.getAckCommitted()
                    || !reply.getStatus().equals("WRITE_COMPLETED")
                    || !reply.getErrorCode().isEmpty()
                    || !reply.getOperationId().equals(request.getOperationId())
                    || !reply.getCallId().equals(delivery.command().callId().value())
                    || reply.getCallVersion() != delivery.callVersion()
                    || reply.getResult().size() > 8192)
                  throw new IllegalStateException("RESYNC_REQUIRED");
                var receipt =
                    JSON.readValue(reply.getResult().toByteArray(), RelayWriteReceipt.class);
                if (!receipt.matches(delivery.command())
                    || receipt.callVersion() != delivery.callVersion())
                  throw new IllegalStateException("RESYNC_REQUIRED");
                flight.logical.complete(receipt);
              } catch (Exception invalid) {
                flight.logical.completeExceptionally(invalid);
              }
            }

            @Override
            public void onCompleted() {
              if (terminal.compareAndSet(false, true))
                try {
                  flight.logical.completeExceptionally(
                      new IllegalStateException("Missing relay receipt"));
                } finally {
                  flight.ended();
                }
            }

            @Override
            public void onError(Throwable failure) {
              if (terminal.compareAndSet(false, true))
                try {
                  flight.logical.completeExceptionally(failure);
                } finally {
                  flight.ended();
                }
            }
          };
      try {
        stub.deliverRelay(
            request.toBuilder()
                .setRemainingBudgetMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)))
                .build(),
            observer);
      } catch (Throwable unknown) {
        flight.logical.completeExceptionally(unknown);
      } // Original stream receipt remains until a real terminal callback.
    } catch (Exception notStarted) {
      flight.logical.completeExceptionally(notStarted);
    } finally {
      flight.ended();
    }
    return flight.operation();
  }

  public synchronized CompletionStage<Void> settleAdmitted() {
    return CompletableFuture.allOf(
            active.stream().map(f -> f.physical).toArray(CompletableFuture[]::new))
        .minimalCompletionStage();
  }

  public synchronized CompletionStage<Void> drain() {
    if (!closed) {
      closed = true;
      channels.values().forEach(ManagedChannel::shutdown);
      RpcTransportDrain.await(settled, List.copyOf(channels.values()), drained, callbacks);
      if (active.isEmpty()) settled.complete(null);
    }
    return drained.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
