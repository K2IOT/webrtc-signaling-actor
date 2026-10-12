package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.observability.*;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Native non-blocking gateway adapter. Original JWT is independently verified at session home. */
public final class NativeGatewayServices implements GatewayServices {
  public record Home(String cell, long directoryEpoch) {
    public Home {
      Objects.requireNonNull(cell);
      if (directoryEpoch <= 0) throw new IllegalArgumentException("Invalid directory epoch");
    }
  }

  @FunctionalInterface
  public interface Commands {
    CompletionStage<String> execute(CallCommand command, Duration budget);
  }

  @FunctionalInterface
  public interface ContextCommands {
    CompletionStage<String> execute(
        CallCommand command,
        SessionRepository.Route route,
        String originalVerifiedToken,
        Duration budget);
  }

  private OperationObservation observation = OperationObservation.NONE;

  public NativeGatewayServices observe(OperationObservation observation) {
    this.observation = Objects.requireNonNull(observation);
    return this;
  }

  private final ContextCommands contextCommands;
  private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
  private final GatewayBootController boot;
  private final BoundedTokenVerifier verifier;
  private final BiFunction<AuthPrincipal, Instant, AuthorizationStatus> security;
  private final Function<UserId, Home> homes;
  private final CellRpcClient client;
  private final Commands commands;
  private volatile RuntimeCapabilities capabilities = RuntimeCapabilities.DENY;

  public NativeGatewayServices compatibility(RuntimeCapabilities value) {
    capabilities = Objects.requireNonNull(value);
    return this;
  }

  @Override
  public boolean supportsInitialIceReceipts() {
    var identity = boot.identity();
    return currentBoot()
        && capabilities.permits(identity.cell(), RuntimeCapabilities.NATIVE_INITIAL_ICE_RECEIPT_V1)
        && capabilities.permitsGateway(
            identity.cell(),
            identity.gatewayId(),
            identity.bootId(),
            RuntimeCapabilities.NATIVE_INITIAL_ICE_RECEIPT_V1);
  }

  private void requireIceCapability(CallCommand command) {
    if (InitialIceReceipt.requiresCapability(command)
        && (!supportsInitialIceReceipts()
            || !capabilities.permits(
                command.callId().coordinatorCell(),
                RuntimeCapabilities.NATIVE_INITIAL_ICE_RECEIPT_V1)))
      throw new IllegalStateException("Initial ICE receipt capability unavailable");
  }

  public NativeGatewayServices(
      GatewayBootController boot,
      BoundedTokenVerifier verifier,
      BiFunction<AuthPrincipal, Instant, AuthorizationStatus> cachedSecurity,
      Function<UserId, Home> cachedHomes,
      CellRpcClient client,
      Commands commands) {
    this.contextCommands = null;
    this.boot = Objects.requireNonNull(boot);
    this.verifier = Objects.requireNonNull(verifier);
    security = Objects.requireNonNull(cachedSecurity);
    homes = Objects.requireNonNull(cachedHomes);
    this.client = Objects.requireNonNull(client);
    this.commands = Objects.requireNonNull(commands);
  }

  public NativeGatewayServices(
      GatewayBootController boot,
      BoundedTokenVerifier verifier,
      BiFunction<AuthPrincipal, Instant, AuthorizationStatus> cachedSecurity,
      Function<UserId, Home> cachedHomes,
      CellRpcClient client,
      ContextCommands commands) {
    this.contextCommands = Objects.requireNonNull(commands);
    this.commands = null;
    this.boot = Objects.requireNonNull(boot);
    this.verifier = Objects.requireNonNull(verifier);
    security = Objects.requireNonNull(cachedSecurity);
    homes = Objects.requireNonNull(cachedHomes);
    this.client = Objects.requireNonNull(client);
  }

  public boolean currentBoot() {
    return boot.current();
  }

  public CompletionStage<AuthPrincipal> verify(String token, Instant now) {
    return observed(SignalingMetrics.Operation.AUTH, () -> verifier.verify(token, now));
  }

  public AuthorizationStatus cachedSecurity(AuthPrincipal principal, Instant now) {
    return security.apply(principal, now);
  }

  public CompletionStage<SessionRepository.Route> register(
      AuthPrincipal p, UUID connection, Duration budget) {
    return CompletableFuture.failedFuture(
        new IllegalStateException("Original verified JWT required"));
  }

  public CompletionStage<SessionRepository.Route> refresh(
      SessionRepository.Route route, AuthPrincipal p, Duration budget) {
    return CompletableFuture.failedFuture(
        new IllegalStateException("Original verified JWT required"));
  }

  public CompletionStage<SessionRepository.Route> register(
      AuthPrincipal p, String token, UUID connection, Duration budget) {
    return mutate("REGISTER", p.userId(), token, null, connection, budget)
        .thenApply(value -> decode(value, SessionRepository.Route.class));
  }

  public CompletionStage<SessionRepository.Route> refresh(
      SessionRepository.Route route, AuthPrincipal p, String token, Duration budget) {
    return mutate("REFRESH", p.userId(), token, route, null, budget)
        .thenApply(value -> decode(value, SessionRepository.Route.class));
  }

  public CompletionStage<Void> close(SessionRepository.Route route) {
    return mutate("CLOSE", route.user(), null, route, null, Duration.ofSeconds(2))
        .thenApply(value -> null);
  }

  public CompletionStage<String> command(CallCommand command, Duration budget) {
    requireIceCapability(command);
    if (!currentBoot())
      return CompletableFuture.failedFuture(new IllegalStateException("Gateway boot unavailable"));
    if (commands == null)
      return CompletableFuture.failedFuture(
          new IllegalStateException("Original session provenance required"));
    return observed(operation(command), () -> commands.execute(command, budget));
  }

  public CompletionStage<String> command(
      CallCommand command, SessionRepository.Route route, String token, Duration budget) {
    requireIceCapability(command);
    if (contextCommands == null) return command(command, budget);
    if (!currentBoot()
        || route == null
        || token == null
        || token.isBlank()
        || !route.gatewayId().equals(boot.identity().gatewayId())
        || !route.bootId().equals(boot.identity().bootId())
        || !route.user().equals(command.sender().userId())
        || !route.key().equals(command.sender().key())
        || !route.incarnation().equals(command.sender().incarnation())
        || route.connectionGeneration() != command.sender().connectionGeneration()
        || !route.connectionId().equals(command.sender().connectionId()))
      return CompletableFuture.failedFuture(
          new IllegalStateException("Invalid native session provenance"));
    return observed(
        operation(command), () -> contextCommands.execute(command, route, token, budget));
  }

  private CompletionStage<SessionReply> mutate(
      String type,
      UserId user,
      String token,
      SessionRepository.Route route,
      UUID connection,
      Duration budget) {
    if (!type.equals("CLOSE") && !currentBoot())
      return CompletableFuture.failedFuture(new IllegalStateException("Gateway boot unavailable"));
    var home = Objects.requireNonNull(homes.apply(user));
    if (!home.cell().equals(boot.identity().cell()))
      return CompletableFuture.failedFuture(
          new IllegalStateException("DIRECTORY_REDIRECT_REQUIRED"));
    var request =
        new NativeSessionHandler.Request(
            type,
            boot.identity(),
            token,
            route,
            connection,
            home.directoryEpoch(),
            0,
            UUID.randomUUID());
    var command =
        SessionCommand.newBuilder()
            .setSchemaMajor(1)
            .setDestinationCell(home.cell())
            .setType(type)
            .setOperationId(request.operation().toString())
            .setRemainingBudgetMs(Math.max(1, budget.toMillis()))
            .setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request)))
            .build();
    return observed(
        type.equals("REGISTER")
            ? SignalingMetrics.Operation.REGISTER_SESSION
            : type.equals("REFRESH")
                ? SignalingMetrics.Operation.AUTH_REFRESH
                : SignalingMetrics.Operation.SESSION_CLOSE,
        () ->
            client
                .session(command, budget)
                .thenApply(
                    value -> {
                      if (!value.getAckCommitted()
                          || !value.getStatus().equals("COMMITTED")
                          || !value.getOperationId().equals(command.getOperationId()))
                        throw new CompletionException(
                            new IllegalStateException("Native session outcome unavailable"));
                      return value;
                    }));
  }

  private <T> CompletionStage<T> observed(
      SignalingMetrics.Operation operation, Supplier<CompletionStage<T>> action) {
    var receipt = OperationObservation.safeLogicalStart(observation, operation);
    try (var scope = receipt.activate()) {
      var result = action.get();
      result.whenComplete(
          (value, error) ->
              receipt.logical(error == null ? observedOutcome(value) : observedFailure(error)));
      return result;
    } catch (Exception failure) {
      receipt.logical(observedFailure(failure));
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static SignalingMetrics.Outcome observedFailure(Throwable error) {
    for (int wrappers = 0;
        wrappers < 16
            && (error instanceof CompletionException || error instanceof ExecutionException)
            && error.getCause() != null;
        wrappers++) error = error.getCause();
    if (error instanceof AuthException) return SignalingMetrics.Outcome.FORBIDDEN;
    if (error instanceof RejectedExecutionException) return SignalingMetrics.Outcome.BACKPRESSURE;
    if (error instanceof TimeoutException) return SignalingMetrics.Outcome.DEADLINE_EXCEEDED;
    if (error instanceof CancellationException) return SignalingMetrics.Outcome.OUTCOME_UNKNOWN;
    return SignalingMetrics.Outcome.INTERNAL_ERROR;
  }

  private static SignalingMetrics.Outcome observedOutcome(Object value) {
    if (value instanceof SessionReply reply)
      return NativeRpcTelemetry.outcome(
          reply.getErrorCode().isEmpty() ? reply.getStatus() : reply.getErrorCode());
    if (value instanceof String payload) {
      try {
        if (payload.length() > 81920) return SignalingMetrics.Outcome.INTERNAL_ERROR;
        var wire = JSON.readTree(payload);
        if (wire.has("error"))
          return NativeRpcTelemetry.outcome(wire.path("error").path("code").asText());
        String code = wire.path("result").path("code").asText();
        if (java.util.Set.of(
                "USER_BUSY",
                "CALL_ENDED",
                "TOKEN_EXPIRED",
                "FORBIDDEN",
                "STALE_VERSION",
                "OUTCOME_UNKNOWN",
                "WORKFLOW_PENDING")
            .contains(code)) return NativeRpcTelemetry.outcome(code);
        if (wire.path("ackCommitted").asBoolean(false)) return SignalingMetrics.Outcome.COMMITTED;
        return SignalingMetrics.Outcome.READ;
      } catch (Exception invalid) {
        return SignalingMetrics.Outcome.INTERNAL_ERROR;
      }
    }
    return SignalingMetrics.Outcome.READ;
  }

  private static SignalingMetrics.Operation operation(CallCommand command) {
    try {
      return SignalingMetrics.Operation.valueOf(command.type().name());
    } catch (IllegalArgumentException unsupported) {
      return SignalingMetrics.Operation.QUERY;
    }
  }

  private static <T> T decode(SessionReply reply, Class<T> type) {
    try {
      if (reply.getResult().size() > 81920) throw new IllegalArgumentException();
      return JSON.readValue(reply.getResult().toByteArray(), type);
    } catch (Exception invalid) {
      throw new CompletionException(new IllegalStateException("Invalid native session reply"));
    }
  }
}
