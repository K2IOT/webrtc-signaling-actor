package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.*;
import java.util.Map;
import java.util.Objects;

/** Explicit bounded topology and cell-bound TLS; no discovery or certificate defaults. */
public record NativeCellRpcEnrollment(String environment,Map<String,CellRpcClient.Endpoint> destinations,
        RpcTlsContexts.ClientTls tls,RpcAdmission admission) {
    public NativeCellRpcEnrollment {
        Objects.requireNonNull(tls); Objects.requireNonNull(admission);
        destinations=Map.copyOf(destinations);
        if(environment==null||!environment.matches("[a-z0-9-]{1,32}")||destinations.isEmpty()
                ||destinations.size()>50||destinations.keySet().stream().anyMatch(cell->!cell.matches("[a-z][a-z0-9-]{0,23}")))
            throw new IllegalArgumentException("Invalid native cell RPC enrollment");
    }
}
