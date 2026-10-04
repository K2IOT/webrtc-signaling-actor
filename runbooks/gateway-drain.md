# Gateway drain

Applies to an explicitly approved target namespace/candidate. Local lifecycle tests are not deployment evidence. Budget: **300 seconds**. Keep original session/incarnation/request IDs; reconnect does not invent a new call or reset a healthy PeerConnection.

1. Capture candidate digest, effective configuration fingerprint, current native gateway boot and socket/admission counts. Verify surviving AZ gateways have qualified static headroom. Readiness off and admission shed precede every reconnect notification; remove the gateway from ready-only business service and stop accepting WSS upgrades/AUTH/new INVITEs.
2. `ShutdownCoordinator` sends reconnect notifications in batches of at most **128**, separated by **100ms**. The production hook emits the existing protocol control event and closes via the tracked Netty write/cleanup path. Keep native boot/security checks and bounded renewal/termination resources alive. Never label a socket write as an application receipt.
3. Await admitted work **and actual physical completion**, including JDBC, gRPC/Netty writes and proof continuations. A logical timeout is not cleanup. Drain retained safe termination/reconciliation work; stale generation closes stay conditional at the native home.
4. Finish native releases, transport cleanup and framework departure; close pools only after these positive completion signals. If the deadline or release becomes UNKNOWN, leave DB/renewal resources alive, keep readiness false and preserve native fencing; Kubernetes may enforce the 300s termination deadline. Do not force pool closure to fabricate a successful drain.

Inspect the approved target before changing it:

```bash
kubectl --context "$SIGNALING_KUBE_CONTEXT" -n "$SIGNALING_NAMESPACE" get deployment "$SIGNALING_RELEASE-gateway" -o yaml
kubectl --context "$SIGNALING_KUBE_CONTEXT" -n "$SIGNALING_NAMESPACE" get endpointslices -l "kubernetes.io/service-name=$SIGNALING_RELEASE-gateway"
```

Use the approved deployment rollout to send SIGTERM. Record the readiness-off, last admission, reconnect batches, final actual completion and process-exit timestamps. Acceptance: <=5m; no stale binding delivery, no unbounded aggregate buffers, no missed physical credits, no forced authority extension. Fault and scale acceptance requires raw target-cluster metrics and timelines.
