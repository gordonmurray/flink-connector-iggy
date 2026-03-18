package org.apache.flink.connector.iggy;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.math.BigInteger;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
class IggySdkConnectivityTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));

    private IggyTcpClient connect() {
        return IggyTcpClient.builder().host(HOST).port(PORT)
                .credentials("iggy", "iggy").buildAndLogin();
    }

    @Test
    void shouldConnectAndPollMessages() {
        var polled = connect().messages().pollMessages(
                StreamId.of("crypto"), TopicId.of("prices"),
                Optional.of(0L), Consumer.of(1L),
                PollingStrategy.first(), 10L, false);

        assertFalse(polled.messages().isEmpty());
        var first = polled.messages().get(0);
        assertTrue(first.payload().length > 0);
        assertTrue(new String(first.payload()).contains("\"pair\""));
        assertEquals(BigInteger.ZERO, first.header().offset());
    }

    @Test
    void shouldPollFromExplicitOffset() {
        var polled = connect().messages().pollMessages(
                StreamId.of("crypto"), TopicId.of("prices"),
                Optional.of(0L), Consumer.of(1L),
                PollingStrategy.offset(BigInteger.valueOf(100)), 5L, false);

        assertFalse(polled.messages().isEmpty());
        assertEquals(BigInteger.valueOf(100), polled.messages().get(0).header().offset());
    }
}
