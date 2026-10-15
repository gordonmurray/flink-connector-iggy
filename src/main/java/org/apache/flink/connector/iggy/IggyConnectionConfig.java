package org.apache.flink.connector.iggy;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClientBuilder;

import java.io.Serializable;

/**
 * Immutable connection configuration shared by the reader and enumerator.
 * Eliminates parameter sprawl across constructors.
 */
record IggyConnectionConfig(
        String host,
        int port,
        String username,
        String password,
        boolean tls,
        String tlsCertificatePath
) implements Serializable {

    IggyTcpClient connect() {
        var builder = IggyTcpClient.builder()
                .host(host)
                .port(port)
                .credentials(username, password);
        if (tls) {
            builder.enableTls();
            if (tlsCertificatePath != null) {
                builder.tlsCertificate(tlsCertificatePath);
            }
        }
        // buildAndLogin() without the leak: a login that fails after connecting
        // would otherwise leave the open connection with no owner to close it.
        IggyTcpClient client = builder.build();
        try {
            client.connect();
            client.login();
            return client;
        } catch (RuntimeException e) {
            IggyClients.closeQuietly(client);
            throw e;
        }
    }
}
