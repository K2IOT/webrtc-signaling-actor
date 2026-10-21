package io.webrtc.signaling.app.runtime.local;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.rpc.RpcTlsContexts;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;

/**
 * TLS contexts for one LOCAL_TEST_ONLY container, built only from files the mounted contract names.
 * Every path goes through {@link LocalRuntimeContract#resolve(String)} and every read is bounded
 * ({@link LocalFiles}). Failures are {@link LocalRuntimeFailure}s that carry no path, password or
 * cause: an unreadable file is {@code PATH/MISSING}, unusable content {@code CONTRACT/INVALID}.
 *
 * <p>Where TLSv1.3 is actually enforced. The Netty contexts ({@link #wssServer}, {@link
 * #mutualServer}, {@link #mutualClient}) and the RPC contexts (via {@link RpcTlsContexts}) restrict
 * the protocol list of every engine they create, so a TLSv1.2 peer is rejected. The JDK contexts
 * ({@link #jdkContext}, {@link #jdkTrustOnly}, {@link #jdkMutualClient}) only carry a TLSv1.3
 * <em>default</em>; see {@link LocalTls13Context}. The receiving side must enforce the protocol.
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
                                        stream(c, tls.certificate(), LocalFiles.CERTIFICATE_MAX),
                                        stream(c, tls.privateKey(), LocalFiles.KEY_MAX))
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
                                        stream(c, tls.certificate(), LocalFiles.CERTIFICATE_MAX),
                                        stream(c, tls.privateKey(), LocalFiles.KEY_MAX))
                                .sslProvider(SslProvider.JDK)
                                .trustManager(stream(c, tls.ca(), LocalFiles.CERTIFICATE_MAX))
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
                                        stream(c, tls.certificate(), LocalFiles.CERTIFICATE_MAX),
                                        stream(c, tls.privateKey(), LocalFiles.KEY_MAX))
                                .trustManager(stream(c, tls.ca(), LocalFiles.CERTIFICATE_MAX))
                                .protocols(TLS13)
                                .build());
    }

    /** Actor cell-RPC ingress; the leaf must carry this cell's single actor SPIFFE URI. */
    public static SslContext actorRpcServer(LocalRuntimeContract c) {
        return rpc(
                () ->
                        RpcTlsContexts.server(
                                c.contract().environment(),
                                c.contract().cell(),
                                file(c, c.contract().tls().ca()),
                                file(c, c.contract().tls().certificate()),
                                file(c, c.contract().tls().privateKey())));
    }

    /** Gateway relay server; the leaf must carry this gateway's single SPIFFE URI. */
    public static SslContext gatewayRpcServer(LocalRuntimeContract c) {
        var gateway = c.profile().gatewayId();
        if (gateway == null) throw LocalJson.failure(Role.PROFILE, Category.INVALID);
        return rpc(
                () ->
                        RpcTlsContexts.gatewayServer(
                                c.contract().environment(),
                                c.contract().cell(),
                                gateway,
                                file(c, c.contract().tls().ca()),
                                file(c, c.contract().tls().certificate()),
                                file(c, c.contract().tls().privateKey())));
    }

    /** Per-destination-cell client contexts; each pins the destination's actor identity. */
    public static RpcTlsContexts.ClientTls rpcClients(LocalRuntimeContract c) {
        return rpc(
                () ->
                        RpcTlsContexts.clients(
                                c.contract().environment(),
                                file(c, c.contract().tls().ca()),
                                file(c, c.contract().tls().certificate()),
                                file(c, c.contract().tls().privateKey())));
    }

    /**
     * Actor-to-gateway relay client contexts; each pins the destination gateway's identity. The
     * mounted leaf must be an actor workload.
     */
    public static RpcTlsContexts.GatewayClientTls relayClients(LocalRuntimeContract c) {
        return rpc(
                () ->
                        RpcTlsContexts.gatewayClients(
                                c.contract().environment(),
                                file(c, c.contract().tls().ca()),
                                file(c, c.contract().tls().certificate()),
                                file(c, c.contract().tls().privateKey())));
    }

    /**
     * Mutual-TLS JDK context from the mounted remoting PKCS12 key store and trust store (Pekko
     * management HTTP clients and servers).
     *
     * <p>TLSv1.3 is only the <em>default</em> of engines, sockets and parameters created from this
     * context; a consumer that calls {@code setEnabledProtocols} or {@code setSSLParameters}
     * re-enables whatever it names. Pekko Artery does not use this context: it builds its own
     * from {@code application-actor.conf} ({@code protocol = TLSv1.3}, enforced by
     * {@code ShardingBootstrap.validateProduction}). The receiving side must enforce the protocol.
     */
    public static SSLContext jdkContext(
            LocalRuntimeContract c,
            char[] keyStorePassword,
            char[] keyPassword,
            char[] trustStorePassword) {
        var tls = c.contract().tls();
        var keys = pkcs12(c, tls.remotingKeyStore(), keyStorePassword);
        var trust = pkcs12(c, tls.remotingTrustStore(), trustStorePassword);
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

    /**
     * Client context that trusts only the CA certificates in {@code caPath} and presents no
     * identity. The TLSv1.3 pin is a default only, as for {@link #jdkContext}; callers that verify
     * a hostname apply {@link #httpsEndpointIdentification}.
     */
    public static SSLContext jdkTrustOnly(LocalRuntimeContract c, String caPath) {
        var bytes = LocalFiles.read(c.resolve(caPath), LocalFiles.CERTIFICATE_MAX);
        try {
            var store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            int index = 0;
            for (var certificate :
                    CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(bytes)))
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

    /**
     * Client context that presents the pod identity (the contract's leaf certificate and private
     * key, PEM) and trusts only the contract's CA. The TLSv1.3 pin is a default only, as for
     * {@link #jdkContext}; the server must enforce it, and callers that verify the server's name
     * apply {@link #httpsEndpointIdentification}.
     */
    public static SSLContext jdkMutualClient(LocalRuntimeContract c) {
        if (mutualClient(c) instanceof JdkSslContext jdk) return LocalTls13Context.pin(jdk.context());
        throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
    }

    /**
     * Turns on HTTPS endpoint identification, so the server certificate must name the host the
     * engine or socket was created for. Mutates and returns {@code parameters}; apply it with
     * {@code setSSLParameters}, which also keeps the protocol list the engine already has.
     */
    public static SSLParameters httpsEndpointIdentification(SSLParameters parameters) {
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        return parameters;
    }

    private static KeyStore pkcs12(LocalRuntimeContract c, String name, char[] password) {
        var bytes = LocalFiles.read(c.resolve(name), LocalFiles.STORE_MAX);
        try {
            var store = KeyStore.getInstance("PKCS12");
            store.load(new ByteArrayInputStream(bytes), password);
            return store;
        } catch (IOException | GeneralSecurityException invalid) {
            throw LocalJson.failure(Role.CONTRACT, Category.INVALID);
        }
    }

    /** A bounded read of a mounted file, so unreadable and oversized inputs map uniformly. */
    private static InputStream stream(LocalRuntimeContract c, String name, int maximum) {
        return new ByteArrayInputStream(LocalFiles.read(c.resolve(name), maximum));
    }

    /** Resolved, confined and proven readable and bounded; for APIs that insist on a file. */
    private static File file(LocalRuntimeContract c, String name) {
        var path = c.resolve(name);
        LocalFiles.read(path, LocalFiles.CERTIFICATE_MAX);
        return path.toFile();
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
