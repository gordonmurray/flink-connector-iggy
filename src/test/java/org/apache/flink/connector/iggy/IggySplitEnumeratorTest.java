package org.apache.flink.connector.iggy;

import org.apache.flink.core.io.InputStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IggySplitEnumeratorTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));

    @Test
    @EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
    void shouldDiscoverAllPartitions() {
        var client = org.apache.iggy.client.blocking.tcp.IggyTcpClient.builder()
                .host(HOST).port(PORT).credentials("iggy", "iggy").buildAndLogin();
        var topic = client.topics().getTopic(
                org.apache.iggy.identifier.StreamId.of("crypto"),
                org.apache.iggy.identifier.TopicId.of("prices")).orElseThrow();
        assertEquals(3, topic.partitionsCount().intValue());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
    void shouldReadFromAllPartitions() throws Exception {
        var config = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null);
        var reader = new IggySourceReader<byte[]>(
                new IggySourceReaderTest.StubSourceReaderContext(),
                config, 100L, 5000L, 100, 1L, payload -> payload);
        reader.start();
        reader.addSplits(List.of(
                new IggySplit("crypto", "prices", 0, 0),
                new IggySplit("crypto", "prices", 1, 0),
                new IggySplit("crypto", "prices", 2, 0)));

        var collected = new ArrayList<byte[]>();
        var output = new IggySourceReaderTest.CollectingReaderOutput(collected);

        // Poll multiple times — round-robin serves one split per pollNext call.
        for (var attempt = 0; attempt < 5; attempt++) {
            reader.pollNext(output);
        }
        assertFalse(collected.isEmpty());

        var snapshot = reader.snapshotState(1L);
        assertEquals(3, snapshot.size());
        snapshot.forEach(s -> assertTrue(s.getCurrentOffset() > 0,
                "Partition " + s.getPartitionId() + " should have advanced"));
        reader.close();
    }

    // This test needs no Iggy — always runs.
    @Test
    void shouldSerializeAndDeserializeEnumeratorState() throws Exception {
        var state = new IggyEnumeratorState(
                List.of(new IggySplit("s", "t", 0, 42), new IggySplit("s", "t", 1, 99)),
                List.of(new IggySplit("s", "t", 2, 0)));
        var serializer = new IggyEnumeratorStateSerializer();

        var restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertEquals(2, restored.assignedSplits().size());
        assertEquals(1, restored.unassignedSplits().size());
        assertEquals(42, restored.assignedSplits().get(0).getCurrentOffset());
        assertEquals(99, restored.assignedSplits().get(1).getCurrentOffset());
        assertEquals(0, restored.unassignedSplits().get(0).getCurrentOffset());
        assertEquals(2, restored.unassignedSplits().get(0).getPartitionId());
    }
}
