package org.apache.flink.connector.iggy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Issue #3 end to end against a live Iggy: a reader restored at offset 10 after the
 * topic was deleted and recreated must read the new topic's messages, not skip them.
 * Uses its own throwaway stream.
 */
@EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
class IggyStaleOffsetLiveTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));
    private static final String STREAM = "flink-connector-iggy-stale-offset-test";
    private static final StreamId S = StreamId.of(STREAM);
    private static final TopicId T = TopicId.of("events");

    private IggyTcpClient admin;

    @BeforeEach
    void createTopicWithTenMessages() {
        admin = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null).connect();
        try {
            admin.streams().deleteStream(S);
        } catch (RuntimeException ignored) {
            // left over from an earlier run, or absent
        }
        admin.streams().createStream(STREAM);
        createTopic();
        send("old", 10);
    }

    @AfterEach
    void dropStream() {
        admin.streams().deleteStream(S);
        IggyClients.closeQuietly(admin);
    }

    @Test
    void restoredReaderReadsARecreatedTopicFromTheStart() throws Exception {
        admin.topics().deleteTopic(S, T);
        createTopic();
        send("new", 3);

        var config = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null);
        var reader = new IggySourceReader<byte[]>(new IggySourceReaderTest.StubSourceReaderContext(), config,
                100L, 5000L, 100, 1L, payload -> payload, IggyStaleOffsetPolicy.EARLIEST);
        reader.start();
        reader.addSplits(List.of(new IggySplit(STREAM, "events", 0, 10)));  // as restored from a checkpoint

        var collected = new ArrayList<byte[]>();
        for (int i = 0; i < 5; i++) {
            reader.pollNext(new IggySourceReaderTest.CollectingReaderOutput(collected));
        }
        reader.close();

        assertEquals(List.of("new-0", "new-1", "new-2"), collected.stream().map(String::new).toList());
    }

    private void createTopic() {
        admin.topics().createTopic(S, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO,
                Optional.empty(), "events");
    }

    private void send(String tag, int n) {
        admin.messages().sendMessages(S, T, Partitioning.partitionId(0L),
                IntStream.range(0, n).mapToObj(i -> Message.of(tag + "-" + i)).toList());
    }
}
