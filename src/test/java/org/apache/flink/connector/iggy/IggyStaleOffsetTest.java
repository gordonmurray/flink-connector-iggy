package org.apache.flink.connector.iggy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;

/**
 * A restored offset beyond the end of its partition (topic recreated or purged, issue #3).
 * The fake partition answers like Iggy 0.7.0: an empty batch, never an error, past the end.
 */
class IggyStaleOffsetTest {

    /** A partition holding {@code size} messages at offsets 0..size-1. */
    static class FakePartition implements IggyPoller {
        volatile int size;
        volatile boolean failLookups;
        final AtomicInteger countLookups = new AtomicInteger();

        FakePartition(int size) {
            this.size = size;
        }

        @Override
        public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
            List<Message> batch = new ArrayList<>();
            for (long o = split.getCurrentOffset(); o < size && batch.size() < batchSize; o++) {
                batch.add(message(o));
            }
            long currentOffset = size == 0 ? 0 : size - 1;  // Iggy reports 0 for an empty partition too
            return new PolledMessages((long) split.getPartitionId(), BigInteger.valueOf(currentOffset),
                    (long) batch.size(), batch);
        }

        @Override
        public long messagesCount(IggySplit split) {
            countLookups.incrementAndGet();
            if (failLookups) {
                throw new IllegalStateException("topic lookup failed");
            }
            return size;
        }

        @Override
        public void close() {}
    }

    static Message message(long offset) {
        var template = Message.of("m" + offset).header();
        var header = new MessageHeader(template.checksum(), template.id(), BigInteger.valueOf(offset),
                template.timestamp(), template.originTimestamp(), template.userHeadersLength(), template.payloadLength(),
                template.reserved());
        return new Message(header, ("m" + offset).getBytes(), java.util.Map.of());
    }

    static IggySourceReader<byte[]> reader(IggyPoller partition, IggyStaleOffsetPolicy policy, long splitOffset) {
        return reader(partition, policy, new IggySplit("s", "t", 0, splitOffset));
    }

    static IggySourceReader<byte[]> reader(IggyPoller partition, IggyStaleOffsetPolicy policy, IggySplit split) {
        var config = new IggyConnectionConfig("unused", 0, "u", "p", false, null);
        var reader = new IggySourceReader<byte[]>(new IggySourceReaderTest.StubSourceReaderContext(), config,
                () -> partition, 1L, 1000L, 100, 1L, payload -> payload, policy);
        reader.start();
        reader.addSplits(List.of(split));
        return reader;
    }

    static List<String> pollAll(IggySourceReader<byte[]> reader) throws Exception {
        var collected = new ArrayList<byte[]>();
        var output = new IggySourceReaderTest.CollectingReaderOutput(collected);
        for (int i = 0; i < 5; i++) {
            reader.pollNext(output);
        }
        return collected.stream().map(String::new).toList();
    }

    static long offset(IggySourceReader<byte[]> reader) {
        return reader.snapshotState(1L).get(0).getCurrentOffset();
    }

    @Test
    void earliestRereadsTheRecreatedPartitionInsteadOfSkippingIt() throws Exception {
        // Checkpointed at offset 5; the topic was recreated and now holds 3 messages
        var reader = reader(new FakePartition(3), IggyStaleOffsetPolicy.EARLIEST, 5);
        assertEquals(List.of("m0", "m1", "m2"), pollAll(reader));
        assertEquals(3, offset(reader));
        reader.close();
    }

    @Test
    void latestSkipsToTheEndOfTheRecreatedPartition() throws Exception {
        var reader = reader(new FakePartition(3), IggyStaleOffsetPolicy.LATEST, 5);
        assertEquals(List.of(), pollAll(reader));
        assertEquals(3, offset(reader));
        reader.close();
    }

    @Test
    void failStopsTheTaskWithAnExplanation() {
        var reader = reader(new FakePartition(3), IggyStaleOffsetPolicy.FAIL, 5);
        var e = assertThrows(IOException.class, () -> pollAll(reader));
        assertTrue(e.getMessage().contains("recreated or purged"), e.getMessage());
    }

    @Test
    void emptyRecreatedPartitionAtOffsetOneIsResolvedByMessageCount() throws Exception {
        // currentOffset is 0 for both "empty" and "one message"; the count tells them apart
        var partition = new FakePartition(0);
        var reader = reader(partition, IggyStaleOffsetPolicy.EARLIEST, 1);
        pollAll(reader);
        assertEquals(0, offset(reader), "an empty recreated partition is re-read from 0");
        assertTrue(partition.countLookups.get() >= 1);
        reader.close();
    }

    @Test
    void singleMessagePartitionAtOffsetOneIsNotStale() throws Exception {
        var partition = new FakePartition(1);
        var reader = reader(partition, IggyStaleOffsetPolicy.EARLIEST, 1);
        assertEquals(List.of(), pollAll(reader));
        assertEquals(1, offset(reader));
        assertTrue(partition.countLookups.get() >= 1, "the ambiguous case asks for the message count");
        reader.close();
    }

    @Test
    void purgeWhileAtOffsetOneIsDetectedAndTheNewFirstMessageIsRead() throws Exception {
        var partition = new FakePartition(1);
        var reader = reader(partition, IggyStaleOffsetPolicy.EARLIEST, 1);
        pollAll(reader);
        partition.size = 0;  // purged
        pollAll(reader);
        assertEquals(0, offset(reader));
        partition.size = 1;  // a new message at offset 0
        assertEquals(List.of("m0"), pollAll(reader));
        reader.close();
    }

    @Test
    void latestOnARecreatedPartitionHoldingOnlyOffsetZeroSkipsIt() throws Exception {
        var reader = reader(new FakePartition(1), IggyStaleOffsetPolicy.LATEST, 5);
        assertEquals(List.of(), pollAll(reader));
        assertEquals(1, offset(reader));
        reader.close();
    }

    @Test
    void latestOnAnEmptyRecreatedPartitionStartsAtZero() throws Exception {
        var reader = reader(new FakePartition(0), IggyStaleOffsetPolicy.LATEST, 5);
        assertEquals(List.of(), pollAll(reader));
        assertEquals(0, offset(reader));
        reader.close();
    }

    @Test
    void newSplitAtAConfiguredSpecificOffsetBeyondTheEndIsNotTreatedAsStale() throws Exception {
        var partition = new FakePartition(10);
        var reader = reader(partition, IggyStaleOffsetPolicy.FAIL, new IggySplit("s", "t", 0, 100, true));
        assertEquals(List.of(), pollAll(reader));
        assertEquals(100, offset(reader));
        assertTrue(reader.snapshotState(1L).get(0).isAwaitingConfiguredStart(), "the exemption is checkpointed");
        reader.close();
    }

    @Test
    void restoredSplitThatHappensToSitAtTheConfiguredOffsetIsStillChecked() {
        // Read past its start before, so not awaiting: being at offset 100 again is no exemption
        var reader = reader(new FakePartition(10), IggyStaleOffsetPolicy.FAIL, new IggySplit("s", "t", 0, 100, false));
        assertThrows(IOException.class, () -> pollAll(reader));
    }

    @Test
    void readingTheFirstMessageEndsTheExemption() throws Exception {
        var partition = new FakePartition(3);
        var reader = reader(partition, IggyStaleOffsetPolicy.FAIL, new IggySplit("s", "t", 0, 1, true));
        assertEquals(List.of("m1", "m2"), pollAll(reader));
        assertTrue(!reader.snapshotState(1L).get(0).isAwaitingConfiguredStart());
        reader.close();
    }

    @Test
    void failingSizeLookupsEscalateLikeFailingPolls() {
        var partition = new FakePartition(0);
        partition.failLookups = true;
        var reader = reader(partition, IggyStaleOffsetPolicy.EARLIEST, 1);
        assertThrows(IOException.class, () -> pollAll(reader));
    }

    @Test
    void readerAtTheTailIsNotStale() throws Exception {
        var partition = new FakePartition(10);
        var reader = reader(partition, IggyStaleOffsetPolicy.FAIL, 10);
        assertEquals(List.of(), pollAll(reader));
        assertEquals(10, offset(reader));
        assertEquals(0, partition.countLookups.get());
        reader.close();
    }

    @Test
    void parsesTheTableOption() {
        assertEquals(IggyStaleOffsetPolicy.EARLIEST, IggyDynamicTableFactory.parseStaleOffset("earliest"));
        assertEquals(IggyStaleOffsetPolicy.LATEST, IggyDynamicTableFactory.parseStaleOffset(" LATEST "));
        assertEquals(IggyStaleOffsetPolicy.FAIL, IggyDynamicTableFactory.parseStaleOffset("fail"));
        assertEquals(IggyStaleOffsetPolicy.EARLIEST, IggyDynamicTableFactory.parseStaleOffset(null));
        assertThrows(IllegalArgumentException.class, () -> IggyDynamicTableFactory.parseStaleOffset("never"));
    }
}
