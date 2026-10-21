package io.webrtc.signaling.app.runtime.local;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.rpc.RpcTlsContexts;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;

/**
 * TLS contexts for one LOCAL_TEST_ONLY container, built only from files the mounted contract names.
 * Every path goes through {@link LocalRuntimeContract#resolve(String)}, every context is TLSv1.3
 * only, and failures are {@link LocalRuntimeFailure}s that carry no path, password or cause.
 *
 * <p>The RPC contexts are the existing {@link RpcTlsContexts}: the exactly-one-SPIFFE-URI check
 * lives there and is not reimplemented. Passwords for PKCS12 stores are supplied by the caller,
 * which reads them from mounted files.
 */
public final class LocalTls {
    private static final String TLS13 = "TLSv1.3";

    private LocalTls() {
    }

    /** WSS/HTTPS server: the pod leaf, TLSv1.3, no client authentication. */
    public static SslContext wssServer(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return build(
                () ->
                        SslContextBuilder.forServer(
                                        c.resolve(tls.certificate()).toFile(), c.resolve(tls.privateKey()).toFile())
                                .sslProvider(SslProvider.JDK)
                                .clientAuth(ClientAuth.NONE)
                                .protocols(TLS13)
                                .build());
    }

    /** Private mutual-TLS server: a client certificate from the mounted CA is required. */
    public static SslContext mutualServer(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return build(
                () ->
                        SslContextBuilder.forServer(
                                        c.resolve(tls.certificate()).toFile(), c.resolve(tls.privateKey()).toFile())
                                .sslProvider(SslProvider.JDK)
                                .trustManager(c.resolve(tls.ca()).toFile())
                                .clientAuth(ClientAuth.REQUIRE)
                                .protocols(TLS13)
                                .build());
    }

    /** Mutual-TLS client presenting the pod leaf and trusting only the mounted CA. */
    public static SslContext mutualClient(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return build(
                () ->
                        SslContextBuilder.forClient()
                                .sslProvider(SslProvider.JDK)
                                .keyManager(
                                        c.resolve(tls.certificate()).toFile(), c.resolve(tls.privateKey()).toFile())
                                .trustManager(c.resolve(tls.ca()).toFile())
                                .protocols(TLS13)
                                .build());
    }

    /** Actor cell-RPC ingress; the leaf must carry this cell's single actor SPIFFE URI. */
    public static SslContext actorRpcServer(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return rpc(
                () ->
                        RpcTlsContexts.server(
                                c.contract().environment(),
                                c.contract().cell(),
                                c.resolve(tls.ca()).toFile(),
                                c.resolve(tls.certificate()).toFile(),
                                c.resolve(tls.privateKey()).toFile()));
    }

    /** Gateway relay server; the leaf must carry this gateway's single SPIFFE URI. */
    public static SslContext gatewayRpcServer(LocalRuntimeContract c) {
        var gateway = c.profile().gatewayId();
        if (gateway == null) throw LocalJson.failure(Role.PROFILE, Category.INVALID);
        var tls = c.contract().tls();
        return rpc(
                () ->
                        RpcTlsContexts.gatewayServer(
                                c.contract().environment(),
                                c.contract().cell(),
                                gateway,
                                c.resolve(tls.ca()).toFile(),
                                c.resolve(tls.certificate()).toFile(),
                                c.resolve(tls.privateKey()).toFile()));
    }

    /** Per-destination-cell client contexts; each pins the destination's actor identity. */
    public static RpcTlsContexts.ClientTls rpcClients(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return rpc(
                () ->
                        RpcTlsContexts.clients(
                                c.contract().environment(),
                                c.resolve(tls.ca()).toFile(),
                                c.resolve(tls.certificate()).toFile(),
                                c.resolve(tls.privateKey()).toFile()));
    }

    /** Actor-to-gateway relay client contexts; each pins the destination gateway's identity. */
    public static RpcTlsContexts.GatewayClientTls relayClients(LocalRuntimeContract c) {
        var tls = c.contract().tls();
        return rpc(
                () ->
                        RpcTlsContexts.gatewayClients(
                                c.contract().environment(),
                                c.resolve(tls.ca()).toFile(),
                                c.resolve(tls.certificate()).toFile(),
                                c.resolve(tls.privateKey()).toFile()));
    }

    /**
     * Mutual-TLS JDK context from the mounted remoting PKCS12 key store and trust store (Artery and
     * management).
     */
    public static SSLContext jdkContext(
            LocalRuntimeContract c,
            char[] keyStorePassword,
            char[] keyPassword,
            char[] trustStorePassword) {
        var tls = c.contract().tls();
        var keys = pkcs12(c.resolve(tls.remotingKeyStore()), keyStorePassword);
        var trust = pkcs12(c.resolve(tls.remotingTrustStore()), trustStorePassword);
        try {
            var keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagers.init(keys, keyPassword);
            var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trust);
            var context = SSLContext.getInstance(TLS13);
            context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
            return LocalTls13Context.pin(context);
        } catch (GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** Client context that trusts only the CA certificates in {@code caPath} and presents nothing. */
    public static SSLContext jdkTrustOnly(LocalRuntimeContract c, String caPath) {
        var file = c.resolve(caPath);
        try (var input = Files.newInputStream(file)) {
            var store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            int index = 0;
            for (var certificate : CertificateFactory.getInstance("X.509").generateCertificates(input))
                store.setCertificateEntry("ca-" + index++, certificate);
            if (index == 0) throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
            var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            var context = SSLContext.getInstance(TLS13);
            context.init(null, trust.getTrustManagers(), null);
            return LocalTls13Context.pin(context);
        } catch (IOException | GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    private static KeyStore pkcs12(java.nio.file.Path file, char[] password) {
        try (var input = Files.newInputStream(file)) {
            var store = KeyStore.getInstance("PKCS12");
            store.load(input, password);
            return store;
        } catch (IOException | GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    private interface Netty {
        SslContext build() throws Exception;
    }

    private interface Rpc<T> {
        T build() throws Exception;
    }

    private static SslContext build(Netty builder) {
        try {
            return builder.build();
        } catch (LocalRuntimeFailure failure) {
            throw failure;
        } catch (Exception invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** {@link RpcTlsContexts} rejects a wrong identity with IllegalArgumentException. */
    private static <T> T rpc(Rpc<T> builder) {
        try {
            return builder.build();
        } catch (LocalRuntimeFailure failure) {
            throw failure;
        } catch (IllegalArgumentException mismatch) {
            throw LocalJson.failure(Role.IDENTITY, Category.IDENTITY_MISMATCH);
        } catch (Exception invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }
}
