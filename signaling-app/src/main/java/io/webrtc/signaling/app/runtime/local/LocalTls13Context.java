package io.webrtc.signaling.app.runtime.local;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;

/**
 * An initialised JDK context whose engines, sockets and parameters start with TLSv1.3 only. The
 * stock {@code TLSv1.3} context still enables TLSv1.2 by default, so the pin is applied here and a
 * consumer cannot forget it. This adds no cryptography: it only narrows the enabled protocols.
 */
final class LocalTls13Context extends SSLContext {
    static final String[] PROTOCOLS = {"TLSv1.3"};

    private LocalTls13Context(SSLContext delegate) {
        super(new Pinned(delegate), delegate.getProvider(), delegate.getProtocol());
    }

    static SSLContext pin(SSLContext initialised) {
        return new LocalTls13Context(initialised);
    }

    private static SSLParameters pinned(SSLParameters parameters) {
        parameters.setProtocols(PROTOCOLS);
        return parameters;
    }

    private static SSLEngine pinned(SSLEngine engine) {
        engine.setEnabledProtocols(PROTOCOLS);
        return engine;
    }

    private static Socket pinned(Socket socket) {
        ((SSLSocket) socket).setEnabledProtocols(PROTOCOLS);
        return socket;
    }

    private static ServerSocket pinned(ServerSocket socket) {
        ((SSLServerSocket) socket).setEnabledProtocols(PROTOCOLS);
        return socket;
    }

    private static final class Pinned extends SSLContextSpi {
        private final SSLContext delegate;

        Pinned(SSLContext delegate) {
            this.delegate = delegate;
        }

        @Override
        protected void engineInit(
                javax.net.ssl.KeyManager[] keys, javax.net.ssl.TrustManager[] trust, SecureRandom random) {
            throw new UnsupportedOperationException("Already initialised");
        }

        @Override
        protected SSLSocketFactory engineGetSocketFactory() {
            return new Sockets(delegate.getSocketFactory());
        }

        @Override
        protected SSLServerSocketFactory engineGetServerSocketFactory() {
            return new ServerSockets(delegate.getServerSocketFactory());
        }

        @Override
        protected SSLEngine engineCreateSSLEngine() {
            return pinned(delegate.createSSLEngine());
        }

        @Override
        protected SSLEngine engineCreateSSLEngine(String host, int port) {
            return pinned(delegate.createSSLEngine(host, port));
        }

        @Override
        protected SSLSessionContext engineGetServerSessionContext() {
            return delegate.getServerSessionContext();
        }

        @Override
        protected SSLSessionContext engineGetClientSessionContext() {
            return delegate.getClientSessionContext();
        }

        @Override
        protected SSLParameters engineGetDefaultSSLParameters() {
            return pinned(delegate.getDefaultSSLParameters());
        }

        @Override
        protected SSLParameters engineGetSupportedSSLParameters() {
            return pinned(delegate.getSupportedSSLParameters());
        }
    }

    private static final class Sockets extends SSLSocketFactory {
        private final SSLSocketFactory delegate;

        Sockets(SSLSocketFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket() throws IOException {
            return pinned(delegate.createSocket());
        }

        @Override
        public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
                throws IOException {
            return pinned(delegate.createSocket(socket, host, port, autoClose));
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            return pinned(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress local, int localPort)
                throws IOException {
            return pinned(delegate.createSocket(host, port, local, localPort));
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return pinned(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(InetAddress host, int port, InetAddress local, int localPort)
                throws IOException {
            return pinned(delegate.createSocket(host, port, local, localPort));
        }
    }

    private static final class ServerSockets extends SSLServerSocketFactory {
        private final SSLServerSocketFactory delegate;

        ServerSockets(SSLServerSocketFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public ServerSocket createServerSocket() throws IOException {
            return pinned(delegate.createServerSocket());
        }

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            return pinned(delegate.createServerSocket(port));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            return pinned(delegate.createServerSocket(port, backlog));
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress address)
                throws IOException {
            return pinned(delegate.createServerSocket(port, backlog, address));
        }
    }
}
