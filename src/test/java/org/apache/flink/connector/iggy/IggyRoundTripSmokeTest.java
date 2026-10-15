package org.apache.flink.connector.iggy;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ad hoc smoke test for the iggy 0.8.0 compatibility evaluation: creates a
 * stream/topic, sends messages, and polls them back through the connector's
 * own client stack. This is the exact round trip that silently dropped poll
 * responses when the 0.7.0-pinned Java SDK talked to an 0.8.0 server.
 */
@EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
class IggyRoundTripSmokeTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));

    @Test
    void shouldSendAndPollMessages() throws Exception {
        try (IggyTcpClient client = IggyTcpClient.builder().host(HOST).port(PORT)
                .credentials("iggy", "iggy").buildAndLogin()) {

            var streamId = StreamId.of("smoke_stream");
            var topicId = TopicId.of("smoke_topic");

            try {
                client.streams().deleteStream(streamId);
            } catch (Exception ignored) {
                // stream didn't exist from a previous run — fine
            }

            client.streams().createStream("smoke_stream");
            client.topics().createTopic(streamId, 1L, CompressionAlgorithm.None,
                    java.math.BigInteger.ZERO, java.math.BigInteger.ZERO,
                    Optional.empty(), "smoke_topic");

            client.messages().sendMessages(streamId, topicId,
                    Partitioning.partitionId(0L),
                    List.of(Message.of("hello-0.8")));

            var polled = client.messages().pollMessages(
                    streamId, topicId,
                    Optional.of(0L), Consumer.of(1L),
                    PollingStrategy.first(), 10L, false);

            assertFalse(polled.messages().isEmpty(), "expected at least one polled message");
            Message first = polled.messages().get(0);
            assertEquals("hello-0.8", new String(first.payload(), StandardCharsets.UTF_8));
        }
    }
}
