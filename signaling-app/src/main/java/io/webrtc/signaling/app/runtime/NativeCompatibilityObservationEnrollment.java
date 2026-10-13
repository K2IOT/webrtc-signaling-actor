package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.GatewayRelayRpcClient;

import java.util.Objects;
import java.util.Set;

/**
 * Trusted topology/build enrollment. No descriptor is accepted from public commands.
 */
public record NativeCompatibilityObservationEnrollment(
        String cell,
        NativeCompatibilityEnrollment.BuildIdentity installedBuild,
        NativeCompatibilityEnrollment.ApprovedWindow approvedWindow,
        Set<String> cells,
        Set<GatewayRelayRpcClient.Target> gateways) {
    public NativeCompatibilityObservationEnrollment {
        Objects.requireNonNull(installedBuild);
        Objects.requireNonNull(approvedWindow);
        cells = Set.copyOf(cells);
        gateways = Set.copyOf(gateways);
        var admittedCells = cells;
        if (cell == null
                || !cell.matches("[a-z0-9][a-z0-9-]{0,62}")
                || cells.isEmpty()
                || cells.size() > 50
                || !cells.contains(cell)
                || gateways.size() > 128
                || cells.stream().anyMatch(c -> !c.matches("[a-z0-9][a-z0-9-]{0,62}"))
                || gateways.stream().anyMatch(g -> !admittedCells.contains(g.cell()))
                || !NativeCompatibility.IMPLEMENTED.containsAll(installedBuild.implementedFeatures())
                || !approvedWindow.contains(installedBuild.major(), installedBuild.minor()))
            throw new IllegalArgumentException("Incomplete bounded native observation enrollment");
    }
}
