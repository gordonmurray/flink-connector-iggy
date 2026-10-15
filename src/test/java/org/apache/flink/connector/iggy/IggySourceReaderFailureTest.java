package org.apache.flink.connector.iggy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;

/** Failure handling of the reader, with fake pollers instead of a live Iggy. */
class IggySourceReaderFailureTest {

    private static final long TIMEOUT_MS = 100;

    /** A poller that blocks until interrupted, like the SDK waiting for a response that never comes. */
    static class HangingPoller implements IggyPoller {
        final CountDownLatch interrupted = new CountDownLatch(1);
        volatile boolean closed;

        @Override
        public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw new RuntimeException(e);
            }
            return null;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** A poller that returns one message per poll. */
    static class WorkingPoller implements IggyPoller {
        volatile boolean closed;

        @Override
        public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
            List<Message> messages = List.of(Message.of("payload"));
            return new PolledMessages((long) split.getPartitionId(), BigInteger.ZERO, 1L, messages);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /** A poller whose poll throws, like a broken connection. */
    static class FailingPoller implements IggyPoller {
        volatile boolean closed;

        @Override
        public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
            throw new IllegalStateException("connection reset");
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    static IggySourceReader<byte[]> reader(Deque<IggyPoller> pollers) {
        return reader(() -> {
            IggyPoller next = pollers.poll();
            if (next == null) {
                throw new IllegalStateException("no more pollers");
            }
            return next;
        });
    }

    static IggySourceReader<byte[]> reader(Supplier<IggyPoller> connector) {
        var config = new IggyConnectionConfig("unused", 0, "u", "p", false, null);
        var reader = new IggySourceReader<byte[]>(new IggySourceReaderTest.StubSourceReaderContext(),
                config, connector, 1L, TIMEOUT_MS, 10, 1L, payload -> payload,
                IggyStaleOffsetPolicy.EARLIEST);
        reader.start();
        reader.addSplits(Collections.singletonList(new IggySplit("s", "t", 0, 0)));
        return reader;
    }

    static InputStatus poll(IggySourceReader<byte[]> reader) throws Exception {
        return reader.pollNext(new IggySourceReaderTest.CollectingReaderOutput(new ArrayList<>()));
    }

    @Test
    void stalledPollIsInterruptedAndTheConnectionReplaced() throws Exception {
        var hanging = new HangingPoller();
        var working = new WorkingPoller();
        var reader = reader(new ArrayDeque<>(List.of(hanging, working)));

        long start = System.nanoTime();
        assertEquals(InputStatus.NOTHING_AVAILABLE, poll(reader));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5 * TIMEOUT_MS,
                "a stalled poll must not block the mailbox thread past the timeout");
        assertTrue(hanging.interrupted.await(1, TimeUnit.SECONDS), "the stuck poll thread is interrupted");
        assertTrue(hanging.closed, "the stalled connection is closed, never reused");

        assertEquals(InputStatus.MORE_AVAILABLE, poll(reader), "the next poll uses a fresh connection");
        assertEquals(1, reader.snapshotState(1L).get(0).getCurrentOffset());
        reader.close();
        assertTrue(working.closed);
    }

    @Test
    void repeatedStallsFailTheTaskInsteadOfLookingEmpty() throws Exception {
        var pollers = new ArrayDeque<IggyPoller>();
        for (int i = 0; i < IggySourceReader.MAX_CONSECUTIVE_FAILURES; i++) {
            pollers.add(new HangingPoller());
        }
        var all = List.copyOf(pollers);
        var reader = reader(pollers);

        for (int i = 1; i < IggySourceReader.MAX_CONSECUTIVE_FAILURES; i++) {
            assertEquals(InputStatus.NOTHING_AVAILABLE, poll(reader));
        }
        assertThrows(IOException.class, () -> poll(reader));
        all.forEach(p -> assertTrue(((HangingPoller) p).closed));
        reader.close();
    }

    @Test
    void failedPollDiscardsTheConnection() throws Exception {
        var failing = new FailingPoller();
        var reader = reader(new ArrayDeque<>(List.of(failing, new WorkingPoller())));
        assertEquals(InputStatus.NOTHING_AVAILABLE, poll(reader));
        assertTrue(failing.closed);
        assertEquals(InputStatus.MORE_AVAILABLE, poll(reader));
        reader.close();
    }

    @Test
    void successResetsTheFailureCount() throws Exception {
        // Every other poll fails; never MAX_CONSECUTIVE_FAILURES in a row, so the task survives
        var calls = new AtomicInteger();
        var reader = reader(() -> new IggyPoller() {
            @Override
            public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
                if (calls.incrementAndGet() % 2 == 1) {
                    throw new IllegalStateException("connection reset");
                }
                return new WorkingPoller().poll(split, consumerId, batchSize);
            }

            @Override
            public void close() {}
        });
        for (int i = 0; i < 10; i++) {
            assertEquals(InputStatus.NOTHING_AVAILABLE, poll(reader));
            assertEquals(InputStatus.MORE_AVAILABLE, poll(reader));
        }
        reader.close();
    }

    @Test
    void oneFailingSplitCannotStarveTheOthersAndStillEscalates() throws Exception {
        // Partition 0 empty, partition 1 always fails, partition 2 always has data
        Supplier<IggyPoller> connector = () -> new IggyPoller() {
            @Override
            public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
                return switch (split.getPartitionId()) {
                    case 1 -> throw new IllegalStateException("partition 1 broken");
                    case 2 -> new WorkingPoller().poll(split, consumerId, batchSize);
                    default -> new PolledMessages(0L, BigInteger.ZERO, 0L, List.of());
                };
            }

            @Override
            public void close() {}
        };
        var reader = reader(connector);
        reader.addSplits(List.of(new IggySplit("s", "t", 1, 0), new IggySplit("s", "t", 2, 0)));

        boolean partition2Read = false;
        IOException failure = null;
        for (int i = 0; i < 20 && failure == null; i++) {
            try {
                partition2Read |= poll(reader) == InputStatus.MORE_AVAILABLE;
            } catch (IOException e) {
                failure = e;
            }
        }
        assertTrue(partition2Read, "the split behind the failing one is still read");
        assertTrue(failure != null && failure.getMessage().contains("s:t:1"),
                "the failing split escalates even though its neighbours succeed: " + failure);
        reader.close();
    }

    @Test
    void connectThatNeverAnswersIsBoundedAndEscalates() throws Exception {
        var reader = reader(() -> {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        for (int i = 1; i < IggySourceReader.MAX_CONSECUTIVE_FAILURES; i++) {
            long start = System.nanoTime();
            assertEquals(InputStatus.NOTHING_AVAILABLE, poll(reader));
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5 * TIMEOUT_MS);
        }
        assertThrows(IOException.class, () -> poll(reader));
        reader.close();
    }

    @Test
    void closeQuietlyIgnoresClientsItDoesNotRecognise() {
        assertEquals(0, IggyClients.closeQuietly(null));
        assertEquals(0, IggyClients.closeQuietly(new Object()));
    }
}
