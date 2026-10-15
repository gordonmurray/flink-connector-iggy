package org.apache.flink.connector.iggy;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Releases an {@link IggyTcpClient}'s TCP connection.
 *
 * <p>Iggy Java SDK 0.7.0 gives {@code IggyTcpClient} no close method. Its per-area
 * sub-clients share an {@code InternalTcpClient} whose reactor-netty {@code Connection}
 * is a private field, so dropping the client leaks the open connection and its
 * direct buffers. A task that restarts inside the same TaskManager JVM leaks one
 * connection per restart until direct memory runs out.
 *
 * <p>Until the SDK is closeable this disposes the connection reflectively. If a later
 * SDK implements {@link AutoCloseable}, that is used instead.
 */
final class IggyClients {

    private static final Logger LOG = LoggerFactory.getLogger(IggyClients.class);
    private static final String INTERNAL_CLIENT = "org.apache.iggy.client.blocking.tcp.InternalTcpClient";
    private static final String DISPOSABLE_CHANNEL = "reactor.netty.DisposableChannel";
    private static final Duration DISPOSE_TIMEOUT = Duration.ofSeconds(2);

    private IggyClients() {}

    /** Close the client's connection. Never throws; returns the number of connections disposed. */
    static int closeQuietly(Object client) {
        if (client == null || !client.getClass().getName().startsWith("org.apache.iggy.")) {
            return 0;
        }
        try {
            if (client instanceof AutoCloseable closeable) {
                closeable.close();
                return 1;
            }
            Set<Object> internals = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Field f : client.getClass().getDeclaredFields()) {
                Object sub = read(f, client);
                if (sub == null) {
                    continue;
                }
                for (Field g : sub.getClass().getDeclaredFields()) {
                    if (g.getType().getName().equals(INTERNAL_CLIENT)) {
                        Object internal = read(g, sub);
                        if (internal != null) {
                            internals.add(internal);
                        }
                    }
                }
            }
            if (internals.isEmpty()) {
                LOG.warn("Iggy client {} has no {} field; the SDK layout changed, so its connection "
                        + "cannot be closed and will leak", client.getClass().getName(), INTERNAL_CLIENT);
                return 0;
            }
            int disposed = 0;
            for (Object internal : internals) {
                Field conn = internal.getClass().getDeclaredField("connection");
                Object channel = read(conn, internal);  // reactor.netty.Connection, a runtime-only dependency
                if (channel == null) {
                    continue;  // connect() never completed
                }
                // Invoke through the public interface: the implementation class need not be public
                Class<?> api = Class.forName(DISPOSABLE_CHANNEL, false, channel.getClass().getClassLoader());
                if (!(Boolean) api.getMethod("isDisposed").invoke(channel)) {
                    api.getMethod("disposeNow", Duration.class).invoke(channel, DISPOSE_TIMEOUT);
                    disposed++;
                }
            }
            return disposed;
        } catch (Exception | LinkageError e) {
            LOG.warn("Could not close Iggy client; its connection will leak until the JVM exits", e);
            return 0;
        }
    }

    private static Object read(Field f, Object target) throws IllegalAccessException {
        if (f.getType().isPrimitive() || java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
            return null;
        }
        f.setAccessible(true);
        return f.get(target);
    }
}
