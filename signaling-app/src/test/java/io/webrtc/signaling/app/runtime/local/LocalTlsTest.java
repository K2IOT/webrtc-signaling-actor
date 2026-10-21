package io.webrtc.signaling.app.runtime.local;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.UUID;

import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class LocalTlsTest {
    static final char[] PASSWORD = LocalTestPki.PASSWORD.toCharArray();
    static final String GATEWAY_URI = "spiffe://signaling/local-test-only/cell/c001/gateway/gw-1";
    static final String TRUST_ANCHORS = "path does not chain with any of the trust anchors";

    @TempDir
    static Path pki;
    @TempDir
    static Path rogueDirectory;
    static LocalTestPki authority, rogueAuthority;
    static LocalTestPki.Leaf actor, gateway, rogue;

    @TempDir
    Path mount;
    @TempDir
    Path peerMount;

    @BeforeAll
    static void authority() throws Exception {
        authority = LocalTestPki.create(pki);
        rogueAuthority = LocalTestPki.create(rogueDirectory);
        actor = authority.leaf(pki, "actor", "URI:" + ACTOR_URI + ",DNS:localhost");
        gateway = authority.leaf(pki, "gateway", "URI:" + GATEWAY_URI + ",DNS:localhost");
        rogue = rogueAuthority.leaf(rogueDirectory, "rogue", "URI:" + ACTOR_URI + ",DNS:localhost");
    }

    /** The leaf is installed with {@code issuer}'s CA as the contract's trust anchor. */
    static LocalRuntimeContract contract(Path dir, LocalTestPki issuer, LocalTestPki.Leaf leaf, String gatewayId)
            throws Exception {
        var env = LocalRuntimeFixtures.install(dir, gatewayId == null ? "ACTOR" : "GATEWAY");
        if (gatewayId != null) {
            var profile = LocalRuntimeFixtures.profile(dir, "GATEWAY");
            profile.put("gatewayId", gatewayId);
            profile.put("gatewayBootId", UUID.randomUUID().toString());
            Files.writeString(dir.resolve("process.json"), LocalRuntimeFixtures.JSON.writeValueAsString(profile));
        }
        issuer.install(dir, leaf);
        return LocalRuntimeContract.load(env);
    }

    LocalRuntimeContract contract(LocalTestPki.Leaf leaf) throws Exception {
        return contract(mount, authority, leaf, null);
    }

    static SSLEngine server(SslContext context) {
        return context.newEngine(ByteBufAllocator.DEFAULT);
    }

    static SSLEngine client(SSLContext context, String... protocols) {
        var engine = context.createSSLEngine("localhost", 443);
        engine.setUseClientMode(true);
        if (protocols.length > 0) engine.setEnabledProtocols(protocols);
        return engine;
    }

    /** Every message along the cause chain, lower case, so a test pins the reason, not just a type. */
    static String reason(Throwable failure) {
        var text = new StringBuilder();
        for (var t = failure; t != null; t = t.getCause()) text.append(t.getMessage()).append(" | ");
        return text.toString().toLowerCase(java.util.Locale.ROOT);
    }

    static void assertRejected(java.util.concurrent.Callable<?> handshake, String... reasons) {
        var failure = catchThrowable(handshake::call);
        assertThat(failure).isInstanceOf(SSLException.class);
        for (var expected : reasons) assertThat(reason(failure)).contains(expected);
    }

    @Test
    void wssServerNegotiatesTls13WithoutAskingForAClientCertificate() throws Exception {
        var c = contract(actor);
        var tls = LocalTls.wssServer(c);
        assertThat(tls.isServer()).isTrue();
        assertThat(server(tls).getEnabledProtocols()).containsExactly("TLSv1.3");
        var anonymous = LocalTls.jdkTrustOnly(c, c.contract().tls().ca());
        assertThat(LocalTestTls.handshake(client(anonymous), server(tls))).isEqualTo("TLSv1.3");
    }

    @Test
    void rejectsTlsOlderThan13() throws Exception {
        var c = contract(actor);
        var anonymous = LocalTls.jdkTrustOnly(c, c.contract().tls().ca());
        var identified = LocalTls.jdkMutualClient(c);
        assertRejected(
                () -> LocalTestTls.handshake(client(anonymous, "TLSv1.2"), server(LocalTls.wssServer(c))),
                "protocol_version", "not accepted by server preferences [tlsv1.3]");
        assertRejected(
                () -> LocalTestTls.handshake(client(identified, "TLSv1.2"), server(LocalTls.mutualServer(c))),
                "protocol_version", "not accepted by server preferences [tlsv1.3]");
        assertRejected(
                () -> LocalTestTls.handshake(client(identified, "TLSv1.2"), server(LocalTls.actorRpcServer(c))),
                "protocol_version", "not accepted by server preferences [tlsv1.3]");
    }

    @Test
    void requiresClientCertificateOnServer() throws Exception {
        var c = contract(actor);
        var anonymous = LocalTls.jdkTrustOnly(c, c.contract().tls().ca());
        assertRejected(
                () -> LocalTestTls.handshake(client(anonymous), server(LocalTls.mutualServer(c))),
                "certificate_required", "empty client certificate chain");
        assertRejected(
                () -> LocalTestTls.handshake(client(anonymous), server(LocalTls.actorRpcServer(c))),
                "certificate_required", "empty client certificate chain");
    }

    @Test
    void rejectsClientCertificateFromAnotherCa() throws Exception {
        var c = contract(actor);
        // This client trusts the real CA, so only the server can refuse: it is offered a rogue leaf.
        var intruder = contract(peerMount, authority, rogue, null);
        assertRejected(
                () -> LocalTestTls.handshake(client(LocalTls.jdkMutualClient(intruder)), server(LocalTls.mutualServer(c))),
                "certificate_unknown", TRUST_ANCHORS);
        assertRejected(
                () -> LocalTestTls.handshake(client(LocalTls.jdkMutualClient(intruder)), server(LocalTls.actorRpcServer(c))),
                "certificate_unknown", TRUST_ANCHORS);
    }

    @Test
    void rejectsServerCertificateFromAnotherCa() throws Exception {
        var c = contract(actor);
        var impostor = contract(peerMount, rogueAuthority, rogue, null);
        assertRejected(
                () -> LocalTestTls.handshake(client(LocalTls.jdkMutualClient(c)), server(LocalTls.mutualServer(impostor))),
                "certificate_unknown", TRUST_ANCHORS);
        assertRejected(
                () -> LocalTestTls.handshake(client(LocalTls.jdkMutualClient(c)), server(LocalTls.wssServer(impostor))),
                "certificate_unknown", TRUST_ANCHORS);
    }

    @Test
    void mutualClientsPresentTheMountedIdentity() throws Exception {
        var c = contract(actor);
        var jdk = LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD);
        assertThat(LocalTestTls.handshake(client(jdk), server(LocalTls.mutualServer(c)))).isEqualTo("TLSv1.3");
        assertThat(LocalTestTls.handshake(client(LocalTls.jdkMutualClient(c)), server(LocalTls.mutualServer(c))))
                .isEqualTo("TLSv1.3");
        var netty = LocalTls.mutualClient(c);
        assertThat(netty.isClient()).isTrue();
        var nettyEngine = netty.newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        assertThat(nettyEngine.getEnabledProtocols()).containsExactly("TLSv1.3");
        assertThat(LocalTestTls.handshake(nettyEngine, server(LocalTls.mutualServer(c)))).isEqualTo("TLSv1.3");
    }

    @Test
    void httpsEndpointIdentificationChecksTheServerName() throws Exception {
        var c = contract(actor);
        var context = LocalTls.jdkMutualClient(c);
        var named = context.createSSLEngine("localhost", 443);
        named.setUseClientMode(true);
        named.setSSLParameters(LocalTls.httpsEndpointIdentification(named.getSSLParameters()));
        assertThat(named.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        assertThat(named.getSSLParameters().getProtocols()).containsExactly("TLSv1.3");
        assertThat(LocalTestTls.handshake(named, server(LocalTls.mutualServer(c)))).isEqualTo("TLSv1.3");
        var stranger = context.createSSLEngine("wrong.example.test", 443);
        stranger.setUseClientMode(true);
        stranger.setSSLParameters(LocalTls.httpsEndpointIdentification(stranger.getSSLParameters()));
        assertRejected(
                () -> LocalTestTls.handshake(stranger, server(LocalTls.mutualServer(c))),
                "no subject alternative dns name matching wrong.example.test");
        // Without the helper, the name is not checked: the helper is what adds the guarantee.
        assertThat(LocalTestTls.handshake(client(context), server(LocalTls.mutualServer(c)))).isEqualTo("TLSv1.3");
    }

    @Test
    void jdkContextsDefaultToTls13OnEnginesSocketsAndParameters() throws Exception {
        var c = contract(actor);
        for (var context : new SSLContext[]{
                LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD),
                LocalTls.jdkTrustOnly(c, c.contract().tls().ca()),
                LocalTls.jdkMutualClient(c)}) {
            assertThat(context.createSSLEngine().getEnabledProtocols()).containsExactly("TLSv1.3");
            assertThat(context.createSSLEngine("localhost", 443).getEnabledProtocols()).containsExactly("TLSv1.3");
            assertThat(context.getDefaultSSLParameters().getProtocols()).containsExactly("TLSv1.3");
            assertThat(context.getSupportedSSLParameters().getProtocols()).containsExactly("TLSv1.3");
            try (var socket = (SSLSocket) context.getSocketFactory().createSocket()) {
                assertThat(socket.getEnabledProtocols()).containsExactly("TLSv1.3");
            }
            try (var socket = context.getServerSocketFactory().createServerSocket()) {
                assertThat(((SSLServerSocket) socket).getEnabledProtocols()).containsExactly("TLSv1.3");
            }
        }
    }

    /** Documents the limit: the pin is a default, so the receiving side must enforce TLSv1.3. */
    @Test
    void anExplicitProtocolListOverridesTheDefaultPin() throws Exception {
        var c = contract(actor);
        var context = LocalTls.jdkMutualClient(c);
        var engine = context.createSSLEngine();
        engine.setEnabledProtocols(new String[]{"TLSv1.2"});
        assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
        var parameters = context.createSSLEngine().getSSLParameters();
        parameters.setProtocols(new String[]{"TLSv1.2"});
        var viaParameters = context.createSSLEngine();
        viaParameters.setSSLParameters(parameters);
        assertThat(viaParameters.getEnabledProtocols()).containsExactly("TLSv1.2");
        try (var socket = (SSLSocket) context.getSocketFactory().createSocket()) {
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
            assertThat(socket.getEnabledProtocols()).containsExactly("TLSv1.2");
        }
        // Enforcement is the server's: the same TLSv1.2 client is refused by the Netty server.
        assertRejected(
                () -> LocalTestTls.handshake(client(context, "TLSv1.2"), server(LocalTls.mutualServer(c))),
                "protocol_version");
    }

    @Test
    void jdkContextRejectsWrongStorePasswordWithoutLeakingIt() throws Exception {
        var c = contract(actor);
        var wrong = "TEST_ONLY_WRONG".toCharArray();
        var failure = catchThrowable(() -> LocalTls.jdkContext(c, wrong, PASSWORD, PASSWORD));
        assertThat(failure).isInstanceOf(LocalRuntimeFailure.class);
        assertThat(failure.getMessage()).doesNotContain("TEST_ONLY_WRONG");
        assertThat(failure.getCause()).isNull();
        assertFailure(() -> LocalTls.jdkContext(c, wrong, PASSWORD, PASSWORD), Role.CONTRACT, Category.INVALID);
    }

    @Test
    void trustAnchorsMustBeInsideTheMount() throws Exception {
        var c = contract(actor);
        var outside = Files.writeString(Files.createTempFile("TEST_ONLY_outside", ".crt"), "x");
        try {
            assertFailure(() -> LocalTls.jdkTrustOnly(c, outside.toString()), Role.PATH, Category.PATH_ESCAPE);
        } finally {
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void unreadableMissingOversizedAndGarbageFilesMapTheSameWayInEveryOwner() throws Exception {
        var c = contract(actor);
        Assumptions.assumeTrue(!"root".equals(System.getProperty("user.name")));
        var ca = mount.resolve("ca.crt");
        var leaf = mount.resolve("pod/leaf.crt");
        var key = mount.resolve("pod/leaf.key");
        var store = mount.resolve("pod/remoting.p12");
        var original = Files.readAllBytes(ca);
        // Unreadable: the file is there, the process cannot open it.
        for (var file : new Path[]{ca, leaf, key, store})
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"));
        assertFailure(() -> LocalTls.jdkTrustOnly(c, c.contract().tls().ca()), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.wssServer(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.mutualServer(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.mutualClient(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.jdkMutualClient(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.actorRpcServer(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.rpcClients(c), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD), Role.PATH, Category.MISSING);
        for (var file : new Path[]{ca, leaf, key, store})
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        // Missing: removed after the contract was loaded.
        Files.delete(ca);
        assertFailure(() -> LocalTls.jdkTrustOnly(c, c.contract().tls().ca()), Role.PATH, Category.MISSING);
        assertFailure(() -> LocalTls.mutualServer(c), Role.PATH, Category.MISSING);
        // Oversized: the same category for the bounded CA read everywhere.
        Files.write(ca, new byte[LocalFiles.CERTIFICATE_MAX + 1]);
        assertFailure(() -> LocalTls.jdkTrustOnly(c, c.contract().tls().ca()), Role.CONTRACT, Category.TOO_LARGE);
        assertFailure(() -> LocalTls.mutualServer(c), Role.CONTRACT, Category.TOO_LARGE);
        assertFailure(() -> LocalTls.actorRpcServer(c), Role.CONTRACT, Category.TOO_LARGE);
        // Garbage: readable and small, but not a certificate.
        Files.writeString(ca, "TEST_ONLY_NOT_A_CERTIFICATE");
        assertFailure(() -> LocalTls.jdkTrustOnly(c, c.contract().tls().ca()), Role.CONTRACT, Category.INVALID);
        assertFailure(() -> LocalTls.mutualServer(c), Role.CONTRACT, Category.INVALID);
        Files.write(ca, original);
        assertThat(LocalTls.jdkTrustOnly(c, c.contract().tls().ca())).isNotNull();
    }

    @Test
    void rpcContextsAcceptExactlyOneSpiffeUriForTheLocalCell() throws Exception {
        var c = contract(actor);
        assertThat(LocalTls.actorRpcServer(c)).isNotNull();
        assertThat(LocalTls.rpcClients(c).context("c001")).isNotNull();
        assertThat(LocalTls.relayClients(c).context("c001", "gw-1")).isNotNull();
    }

    @Test
    void rejectsCertificateWithoutExactlyOneSpiffeUri() throws Exception {
        var none = authority.leaf(pki, "none", "DNS:localhost");
        var two =
                authority.leaf(
                        pki, "two", "URI:" + ACTOR_URI + ",URI:" + GATEWAY_URI + ",DNS:localhost");
        var otherCell =
                authority.leaf(
                        pki, "cell", "URI:spiffe://signaling/local-test-only/cell/c002/actor,DNS:localhost");
        var otherEnvironment =
                authority.leaf(pki, "env", "URI:spiffe://signaling/test/cell/c001/actor,DNS:localhost");
        for (var leaf : new LocalTestPki.Leaf[]{none, two, otherCell, otherEnvironment}) {
            var c = contract(leaf);
            assertFailure(() -> LocalTls.actorRpcServer(c), Role.IDENTITY, Category.IDENTITY_MISMATCH);
            // A relay client only needs to be an actor; another cell's actor is not rejected here.
            if (leaf != otherCell)
                assertFailure(() -> LocalTls.relayClients(c), Role.IDENTITY, Category.IDENTITY_MISMATCH);
        }
    }

    @Test
    void gatewayRpcServerNeedsAGatewayWorkloadIdentity() throws Exception {
        // The ACTOR profile has no gateway id, so there is no workload identity to pin.
        var c = contract(actor);
        assertFailure(() -> LocalTls.gatewayRpcServer(c), Role.PROFILE, Category.INVALID);
    }

    @Test
    void gatewayRpcServerPinsTheProfileGatewayId() throws Exception {
        var own = contract(mount, authority, gateway, "gw-1");
        assertThat(LocalTls.gatewayRpcServer(own)).isNotNull();
        assertThat(server(LocalTls.gatewayRpcServer(own)).getEnabledProtocols()).containsExactly("TLSv1.3");
        var otherId = contract(peerMount, authority, gateway, "gw-2");
        assertFailure(() -> LocalTls.gatewayRpcServer(otherId), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void gatewayRpcServerRefusesAnActorLeaf() throws Exception {
        var c = contract(mount, authority, actor, "gw-1");
        assertFailure(() -> LocalTls.gatewayRpcServer(c), Role.IDENTITY, Category.IDENTITY_MISMATCH);
    }

    @Test
    void relayClientsRequireAnActorLeafAndPinTheDestinationGateway() throws Exception {
        var asGateway = contract(mount, authority, gateway, "gw-1");
        assertFailure(() -> LocalTls.relayClients(asGateway), Role.IDENTITY, Category.IDENTITY_MISMATCH);
        var caller = contract(peerMount, authority, actor, null);
        var relay = LocalTls.relayClients(caller);
        assertThat(relay.context("c001", "gw-1")).isNotNull();
        // Destination names are validated by RpcTlsContexts when the context is requested.
        assertThatThrownBy(() -> relay.context("C 001", "gw-1")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> relay.context("c001", "gw 1")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void relayHandshakePinsRemoteGatewayCellAndWorkload() throws Exception {
        var gatewayServer = contract(mount, authority, gateway, "gw-1");
        var caller = contract(peerMount, authority, actor, null);
        var relay = LocalTls.relayClients(caller);
        var serverContext = LocalTls.gatewayRpcServer(gatewayServer);
        assertThat(
                LocalTestTls.handshake(
                        relay.context("c001", "gw-1").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443),
                        server(serverContext)))
                .isEqualTo("TLSv1.3");
        assertRejected(
                () ->
                        LocalTestTls.handshake(
                                relay.context("c001", "gw-2").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443),
                                server(serverContext)),
                "workload identity mismatch");
        assertRejected(
                () ->
                        LocalTestTls.handshake(
                                relay.context("c002", "gw-1").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443),
                                server(serverContext)),
                "workload identity mismatch");
    }

    @Test
    void rpcContextsHandshakeOverTls13() throws Exception {
        var c = contract(actor);
        var peer = LocalTls.rpcClients(c).context("c001").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        var local = server(LocalTls.actorRpcServer(c));
        assertThat(LocalTestTls.handshake(peer, local)).isEqualTo("TLSv1.3");
        var wrongDestination =
                LocalTls.rpcClients(c).context("c002").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        assertRejected(
                () -> LocalTestTls.handshake(wrongDestination, server(LocalTls.actorRpcServer(c))),
                "workload identity mismatch");
    }

    private static void assertFailure(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Role role, Category category) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(role);
                            assertThat(failure.category()).isEqualTo(category);
                            assertThat(failure.getCause()).isNull();
                        });
    }
}
