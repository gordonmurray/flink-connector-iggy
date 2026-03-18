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
        return builder.buildAndLogin();
    }
}
