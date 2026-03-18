package org.apache.flink.connector.iggy;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for split and enumerator state serializers. Runs without Iggy. */
class IggySplitSerializerTest {

    private final IggySplitSerializer serializer = new IggySplitSerializer();

    @Test
    void shouldRoundTripSplit() throws IOException {
        var original = new IggySplit("stream-1", "topic-1", 3, 42L);
        var bytes = serializer.serialize(original);
        var restored = serializer.deserialize(serializer.getVersion(), bytes);
        assertEquals(original, restored);
    }

    @Test
    void shouldPreserveSplitId() throws IOException {
        var split = new IggySplit("s", "t", 7, 0);
        var restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
        assertEquals("s:t:7", restored.splitId());
    }

    @Test
    void shouldHandleUnicodeStreamNames() throws IOException {
        var split = new IggySplit("données", "thème", 0, 100);
        var restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
        assertEquals(split, restored);
    }

    @Test
    void shouldRejectUnknownVersion() {
        assertThrows(IOException.class, () ->
                serializer.deserialize(99, new byte[]{0, 0, 0, 1, 65, 0, 0, 0, 1, 66, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void shouldRoundTripEnumeratorState() throws IOException {
        var stateSerializer = new IggyEnumeratorStateSerializer();
        var state = new IggyEnumeratorState(
                java.util.List.of(new IggySplit("a", "b", 0, 10), new IggySplit("a", "b", 1, 20)),
                java.util.List.of(new IggySplit("a", "b", 2, 0)));

        var bytes = stateSerializer.serialize(state);
        var restored = stateSerializer.deserialize(stateSerializer.getVersion(), bytes);

        assertEquals(2, restored.assignedSplits().size());
        assertEquals(1, restored.unassignedSplits().size());
        assertEquals(10, restored.assignedSplits().get(0).getCurrentOffset());
        assertEquals(20, restored.assignedSplits().get(1).getCurrentOffset());
    }
}
