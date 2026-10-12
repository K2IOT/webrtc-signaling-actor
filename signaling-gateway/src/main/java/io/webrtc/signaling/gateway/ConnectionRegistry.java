package io.webrtc.signaling.gateway;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.storage.SessionRepository;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** One local index per process. Closing an old channel cannot remove a replacement binding. */
public final class ConnectionRegistry {
  public static final AttributeKey<UUID> CONNECTION = AttributeKey.valueOf("signaling.connection");

  static final AttributeKey<Boolean> AUTHENTICATED =
      AttributeKey.valueOf("signaling.authenticated");

  public record Binding(Channel channel, SessionRepository.Route route, AuthPrincipal principal) {}

  private final String gateway;
  private final UUID boot;
  private final int maxUnauthenticated, maxConnections;
  private final Map<UUID, Channel> channels = new HashMap<>();
  private final Map<UUID, Binding> bindings = new ConcurrentHashMap<>();
  private final Map<SessionKey, UUID> sessions = new ConcurrentHashMap<>();
  private final Map<UUID, Integer> scanSlots = new HashMap<>();
  private final AtomicReferenceArray<Binding> scanBindings;
  private final int[] freeSlots;
  private int freeSlotCount;
  private int unauthenticated;
  private volatile long safetyProgressNanos, safetyWindowNanos;

  public ConnectionRegistry(String gateway, UUID boot, int maxUnauthenticated) {
    this(gateway, boot, maxUnauthenticated, 200_000);
  }

  public ConnectionRegistry(String gateway, UUID boot, int maxUnauthenticated, int maxConnections) {
    this.gateway = Objects.requireNonNull(gateway);
    this.boot = Objects.requireNonNull(boot);
    if (maxUnauthenticated < 1
        || maxUnauthenticated > 1000
        || maxConnections < maxUnauthenticated
        || maxConnections > 200_000)
      throw new IllegalArgumentException("Bounded connection admission required");
    this.maxUnauthenticated = maxUnauthenticated;
    this.maxConnections = maxConnections;
    scanBindings = new AtomicReferenceArray<>(maxConnections);
    freeSlots = new int[maxConnections];
    for (int i = 0; i < maxConnections; i++) freeSlots[i] = i;
    freeSlotCount = maxConnections;
  }

  public synchronized UUID attach(Channel channel) {
    if (channels.size() >= maxConnections || unauthenticated >= maxUnauthenticated)
      throw new RejectedExecutionException("SOCKET_OVERLOADED");
    UUID id = UUID.randomUUID();
    channels.put(id, channel);
    scanSlots.put(id, freeSlots[--freeSlotCount]);
    unauthenticated++;
    channel.attr(CONNECTION).set(id);
    channel.closeFuture().addListener(f -> remove(id));
    return id;
  }

  public synchronized boolean bind(
      UUID id, SessionRepository.Route route, AuthPrincipal principal) {
    Channel channel = channels.get(id);
    if (channel == null
        || !safetyFresh()
        || !channel.isActive()
        || GatewayRejection.rejecting(channel)
        || !route.connectionId().equals(id)
        || !gateway.equals(route.gatewayId())
        || !boot.equals(route.bootId())
        || !principal.key().equals(route.key())
        || !principal.userId().equals(route.user())
        || route.connectionGeneration() < 1
        || !route.tokenExpiresAt().equals(principal.expiresAt())) return false;
    UUID previousId = sessions.get(route.key());
    Binding previous = previousId == null ? null : bindings.get(previousId);
    if (previous != null
        && previous.route().incarnation().equals(route.incarnation())
        && previous.route().connectionGeneration() > route.connectionGeneration()) return false;
    if (!bindings.containsKey(id)) unauthenticated--;
    var updated = new Binding(channel, route, principal);
    bindings.put(id, updated);
    scanBindings.set(scanSlots.get(id), updated);
    channel.attr(AUTHENTICATED).set(Boolean.TRUE);
    sessions.put(route.key(), id);
    if (previous != null && !Objects.equals(id, previousId))
      previous
          .channel()
          .eventLoop()
          .execute(
              () ->
                  GatewayRejection.close(
                      previous.channel(), GatewayRejection.Reason.STALE_CONNECTION));
    return true;
  }

  public Binding binding(UUID id) {
    return id == null ? null : bindings.get(id);
  }

  public boolean current(UUID id, SessionRepository.Route route) {
    var binding = binding(id);
    return binding != null
        && safetyFresh()
        && binding.channel().isActive()
        && !GatewayRejection.rejecting(binding.channel())
        && binding.route().equals(route)
        && id.equals(sessions.get(route.key()));
  }

  public synchronized void remove(UUID id) {
    Channel channel = channels.remove(id);
    if (channel == null) return;
    channel.attr(AUTHENTICATED).set(Boolean.FALSE);
    int slot = scanSlots.remove(id);
    scanBindings.set(slot, null);
    freeSlots[freeSlotCount++] = slot;
    var binding = bindings.remove(id);
    if (binding == null) unauthenticated--;
    else sessions.remove(binding.route().key(), id);
  }

  /** The watchdog uses planned progress, so a late scan cannot reset away accumulated lag. */
  void beginSafetySweep(long intervalNanos, long nowNanos) {
    safetyProgressNanos = nowNanos;
    safetyWindowNanos = intervalNanos;
  }

  void safetyProgress(long plannedNanos) {
    safetyProgressNanos = plannedNanos;
  }

  boolean safetyFresh() {
    long window = safetyWindowNanos;
    return window == 0 || System.nanoTime() - safetyProgressNanos <= window;
  }

  int scanCapacity() {
    return maxConnections;
  }

  Binding scanBinding(int slot) {
    return scanBindings.get(slot);
  }

  public synchronized int authenticatedCount() {
    return bindings.size();
  }

  public synchronized int unauthenticatedCount() {
    return unauthenticated;
  }

  public synchronized List<Binding> snapshot() {
    return List.copyOf(bindings.values());
  }

  /** Detach at most one fixed reconnect batch; native routes are closed by channelInactive. */
  public synchronized List<Channel> detachBatch(int maximum) {
    if (maximum < 1 || maximum > 128)
      throw new IllegalArgumentException("Reconnect batch outside bound");
    var ids = new ArrayList<UUID>(maximum);
    var iterator = channels.keySet().iterator();
    while (iterator.hasNext() && ids.size() < maximum) ids.add(iterator.next());
    var selected = new ArrayList<Channel>(ids.size());
    for (var id : ids) {
      selected.add(channels.get(id));
      remove(id);
    }
    return List.copyOf(selected);
  }

  public synchronized int connectionCount() {
    return channels.size();
  }
}
