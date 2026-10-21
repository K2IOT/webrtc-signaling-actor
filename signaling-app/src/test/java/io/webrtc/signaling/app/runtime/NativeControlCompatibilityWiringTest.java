package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.storage.SqlTransactions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Profile;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** The control plane's cell-RPC client and compatibility schema source (spec §8 risk). */
class NativeControlCompatibilityWiringTest {
    @Test
    void cellRpcClientIsConfiguredOnTheControlPlaneToo() {
        assertThat(NativeCellRpcConfiguration.class.getAnnotation(Profile.class).value())
                .containsExactlyInAnyOrder("actor", "gateway", "control");
    }

    @Test
    void controlSchemaGateReadsTheLocalCellDatabaseNeverTheRegionalOne() {
        var local = mock(SqlTransactions.class);
        var regional = mock(SqlTransactions.class);
        var resources = mock(NativeControlDatabaseResources.class);
        when(resources.local()).thenReturn(local);
        when(resources.regional()).thenReturn(regional);
        var single = mock(SqlTransactions.class);
        assertThat(
                NativeCompatibilityObservationConfiguration.schemaSource(
                        resources,
                        () -> {
                            throw new AssertionError("TEST_ONLY: two SQL owners are ambiguous on control");
                        }))
                .isSameAs(local);
        // Actor (one SQL owner) and gateway (none) keep the single-owner lookup.
        assertThat(NativeCompatibilityObservationConfiguration.schemaSource(null, () -> single)).isSameAs(single);
        assertThat(NativeCompatibilityObservationConfiguration.schemaSource(null, () -> null)).isNull();
    }

    @Test
    void controlProcessDrainsItsCellRpcClient() throws Exception {
        var directory = mock(PostgresDirectoryRepository.class);
        when(directory.transactionOwners()).thenReturn(List.of());
        var tokens = mock(BoundedTokenVerifier.class);
        when(tokens.drain()).thenReturn(CompletableFuture.completedFuture(null));
        var business =
                new NativeControlBusinessEnrollment(
                        directory,
                        tokens,
                        (principal, now) -> null,
                        () -> false,
                        mock(ClockSafetyMonitor.class),
                        Duration.ofSeconds(30));
        var client = mock(CellRpcClient.class);
        var drained = new CompletableFuture<Void>();
        when(client.drain()).thenReturn(drained);
        var process =
                new NativeControlProcess(
                        business,
                        tokens,
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        null,
                        List.of(client));
        var stage = process.drain().toCompletableFuture();
        verify(client).drain();
        assertThat(stage).as("waits for the client's physical drain").isNotDone();
        drained.complete(null);
        stage.get(5, TimeUnit.SECONDS);

        // Startup cleanup without a process drains it as well.
        var orphan = mock(CellRpcClient.class);
        when(orphan.drain()).thenReturn(CompletableFuture.completedFuture(null));
        NativeControlProcess.drainUninstalled(List.of(orphan)).toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(orphan).drain();
    }
}
