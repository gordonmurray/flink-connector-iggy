package org.apache.flink.connector.iggy;

import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@EnabledIfEnvironmentVariable(named = "TESTCONTAINERS", matches = "true")
class IggyTestcontainersTest {

    @Container
    static final GenericContainer<?> IGGY = new GenericContainer<>("apache/iggy:latest")
            .withExposedPorts(8090)
            .withEnv("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
            .withEnv("IGGY_ROOT_USERNAME", "iggy")
            .withEnv("IGGY_ROOT_PASSWORD", "iggy")
            .waitingFor(Wait.forListeningPort());

    private static IggyConnectionConfig config;
    private static IggyTcpClient client;

    @BeforeAll
    static void setup() {
        config = new IggyConnectionConfig(
                IGGY.getHost(), IGGY.getMappedPort(8090), "iggy", "iggy", false, null);
        client = config.connect();

        client.streams().createStream("test-stream");
        client.topics().createTopic(
                StreamId.of("test-stream"), 1L, CompressionAlgorithm.None,
                BigInteger.ZERO, BigInteger.ZERO, Optional.empty(), "test-topic");

        var messages = IntStream.range(0, 10)
                .mapToObj(i -> Message.of("{\"id\":" + i + "}"))
                .toList();
        client.messages().sendMessages(
                StreamId.of("test-stream"), TopicId.of("test-topic"),
                Partitioning.partitionId(1L), messages);
    }

    @AfterAll
    static void teardown() { client = null; }

    @Test
    void shouldReadAllMessages() throws Exception {
        var reader = new IggySourceReader<byte[]>(
                new IggySourceReaderTest.StubSourceReaderContext(),
                config, 100L, 100, 1L, payload -> payload);
        reader.start();
        reader.addSplits(Collections.singletonList(new IggySplit("test-stream", "test-topic", 0, 0)));

        var collected = new ArrayList<byte[]>();
        assertEquals(InputStatus.MORE_AVAILABLE,
                reader.pollNext(new IggySourceReaderTest.CollectingReaderOutput(collected)));
        assertEquals(10, collected.size());
        assertTrue(new String(collected.get(0)).contains("\"id\":0"));

        assertEquals(10, reader.snapshotState(1L).get(0).getCurrentOffset());
        reader.close();
    }

    @Test
    void shouldDiscoverPartitions() {
        var topic = client.topics().getTopic(
                StreamId.of("test-stream"), TopicId.of("test-topic")).orElseThrow();
        assertEquals(1, topic.partitionsCount().intValue());
    }
}
