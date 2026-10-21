package io.webrtc.signaling.app.runtime.local;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;

/** In-memory TLS handshake between two engines; no sockets, no timing. */
final class LocalTestTls {
    private LocalTestTls() {
    }

    /** Returns the protocol negotiated by the client; throws {@link SSLException} on rejection. */
    static String handshake(SSLEngine client, SSLEngine server) throws Exception {
        var toServer = ByteBuffer.allocate(1 << 16);
        var toClient = ByteBuffer.allocate(1 << 16);
        client.beginHandshake();
        server.beginHandshake();
        for (int round = 0; round < 50; round++) {
            pump(client, toServer, toClient);
            pump(server, toClient, toServer);
            if (settled(client) && settled(server)) return client.getSession().getProtocol();
        }
        throw new IllegalStateException("TLS handshake stalled");
    }

    private static boolean settled(SSLEngine engine) {
        return engine.getHandshakeStatus() == javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
    }

    /** {@code out} and {@code in} are network buffers kept in write mode between calls. */
    private static void pump(SSLEngine engine, ByteBuffer out, ByteBuffer in) throws Exception {
        var application = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
        boolean progress = true;
        while (progress) {
            progress = false;
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK -> {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) task.run();
                    progress = true;
                }
                case NEED_WRAP -> {
                    var result = engine.wrap(ByteBuffer.allocate(0), out);
                    progress = result.getStatus() == javax.net.ssl.SSLEngineResult.Status.OK;
                }
                case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
                    in.flip();
                    var result = engine.unwrap(in, application);
                    in.compact();
                    application.clear();
                    progress = result.getStatus() == javax.net.ssl.SSLEngineResult.Status.OK;
                }
                default -> {
                }
            }
        }
    }
}
