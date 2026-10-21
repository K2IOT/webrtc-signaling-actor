package io.webrtc.signaling.app.runtime.local;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.webrtc.signaling.app.runtime.local.LocalTestPki.ACTOR_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalTlsTest {
    static final char[] PASSWORD = LocalTestPki.PASSWORD.toCharArray();
    @TempDir
    static Path pki;
    static LocalTestPki authority;
    static LocalTestPki.Leaf actor;

    @TempDir
    Path mount;

    @BeforeAll
    static void authority() throws Exception {
        authority = LocalTestPki.create(pki);
        actor = authority.leaf(pki, "actor", "URI:" + ACTOR_URI + ",DNS:localhost");
    }

    LocalRuntimeContract contract(LocalTestPki.Leaf leaf) throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        authority.install(mount, leaf);
        return LocalRuntimeContract.load(env);
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
        var identified = LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD);
        assertThatThrownBy(
                () -> LocalTestTls.handshake(client(anonymous, "TLSv1.2"), server(LocalTls.wssServer(c))))
                .isInstanceOf(SSLException.class);
        assertThatThrownBy(
                () -> LocalTestTls.handshake(client(identified, "TLSv1.2"), server(LocalTls.mutualServer(c))))
                .isInstanceOf(SSLException.class);
        assertThatThrownBy(
                () -> LocalTestTls.handshake(client(identified, "TLSv1.2"), server(LocalTls.actorRpcServer(c))))
                .isInstanceOf(SSLException.class);
    }

    @Test
    void requiresClientCertificateOnServer() throws Exception {
        var c = contract(actor);
        var anonymous = LocalTls.jdkTrustOnly(c, c.contract().tls().ca());
        assertThatThrownBy(
                () -> LocalTestTls.handshake(client(anonymous), server(LocalTls.mutualServer(c))))
                .isInstanceOf(SSLException.class);
        assertThatThrownBy(
                () -> LocalTestTls.handshake(client(anonymous), server(LocalTls.actorRpcServer(c))))
                .isInstanceOf(SSLException.class);
    }

    @Test
    void mutualClientsPresentTheMountedIdentity() throws Exception {
        var c = contract(actor);
        var jdk = LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD);
        assertThat(LocalTestTls.handshake(client(jdk), server(LocalTls.mutualServer(c))))
                .isEqualTo("TLSv1.3");
        var netty = LocalTls.mutualClient(c);
        assertThat(netty.isClient()).isTrue();
        var nettyEngine = netty.newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        assertThat(nettyEngine.getEnabledProtocols()).containsExactly("TLSv1.3");
        assertThat(LocalTestTls.handshake(nettyEngine, server(LocalTls.mutualServer(c))))
                .isEqualTo("TLSv1.3");
    }

    @Test
    void jdkContextsPinTls13OnEnginesSocketsAndParameters() throws Exception {
        var c = contract(actor);
        for (var context : new SSLContext[]{
                LocalTls.jdkContext(c, PASSWORD, PASSWORD, PASSWORD),
                LocalTls.jdkTrustOnly(c, c.contract().tls().ca())}) {
            assertThat(context.createSSLEngine().getEnabledProtocols()).containsExactly("TLSv1.3");
            assertThat(context.getDefaultSSLParameters().getProtocols()).containsExactly("TLSv1.3");
            assertThat(context.getSupportedSSLParameters().getProtocols()).containsExactly("TLSv1.3");
            try (var socket = (SSLSocket) context.getSocketFactory().createSocket()) {
                assertThat(socket.getEnabledProtocols()).containsExactly("TLSv1.3");
            }
            try (var socket = context.getServerSocketFactory().createServerSocket()) {
                assertThat(((javax.net.ssl.SSLServerSocket) socket).getEnabledProtocols())
                        .containsExactly("TLSv1.3");
            }
        }
    }

    @Test
    void jdkContextRejectsWrongStorePasswordWithoutLeakingIt() throws Exception {
        var c = contract(actor);
        var wrong = "TEST_ONLY_WRONG".toCharArray();
        var failure =
                org.assertj.core.api.Assertions.catchThrowable(
                        () -> LocalTls.jdkContext(c, wrong, PASSWORD, PASSWORD));
        assertThat(failure).isInstanceOf(LocalRuntimeFailure.class);
        assertThat(failure.getMessage()).doesNotContain("TEST_ONLY_WRONG");
        assertThat(failure.getCause()).isNull();
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
    void rpcContextsAcceptExactlyOneSpiffeUriForTheLocalCell() throws Exception {
        var c = contract(actor);
        assertThat(LocalTls.actorRpcServer(c)).isNotNull();
        assertThat(LocalTls.rpcClients(c).context("c001")).isNotNull();
    }

    @Test
    void rejectsCertificateWithoutExactlyOneSpiffeUri() throws Exception {
        var none = authority.leaf(pki, "none", "DNS:localhost");
        var two =
                authority.leaf(
                        pki,
                        "two",
                        "URI:" + ACTOR_URI + ",URI:spiffe://signaling/local-test-only/cell/c001/gateway/gw,DNS:localhost");
        var otherCell =
                authority.leaf(
                        pki,
                        "cell",
                        "URI:spiffe://signaling/local-test-only/cell/c002/actor,DNS:localhost");
        var otherEnvironment =
                authority.leaf(pki, "env", "URI:spiffe://signaling/test/cell/c001/actor,DNS:localhost");
        for (var leaf : new LocalTestPki.Leaf[]{none, two, otherCell, otherEnvironment}) {
            var c = contract(leaf);
            assertFailure(() -> LocalTls.actorRpcServer(c), Role.IDENTITY, Category.IDENTITY_MISMATCH);
        }
    }

    @Test
    void gatewayRpcServerNeedsAGatewayWorkloadIdentity() throws Exception {
        // The ACTOR profile has no gateway id, so there is no workload identity to pin.
        var c = contract(actor);
        assertFailure(() -> LocalTls.gatewayRpcServer(c), Role.PROFILE, Category.INVALID);
    }

    @Test
    void rpcContextsHandshakeOverTls13() throws Exception {
        var c = contract(actor);
        var peer = LocalTls.rpcClients(c).context("c001").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        var local = server(LocalTls.actorRpcServer(c));
        assertThat(LocalTestTls.handshake(peer, local)).isEqualTo("TLSv1.3");
        var wrongDestination =
                LocalTls.rpcClients(c).context("c002").newEngine(ByteBufAllocator.DEFAULT, "localhost", 443);
        assertThatThrownBy(
                () -> LocalTestTls.handshake(wrongDestination, server(LocalTls.actorRpcServer(c))))
                .isInstanceOf(SSLException.class);
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
