package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Pulls data from Iggy partitions using round-robin polling with non-blocking backoff.
 *
 * <p>A poll that fails or gets no response within the poll timeout discards the
 * connection: the SDK matches responses to requests through one FIFO queue per
 * connection, so a late response would otherwise be handed to the next request.
 * The reader reconnects on the next poll, under the same deadline. After
 * {@link #MAX_CONSECUTIVE_FAILURES} failures in a row of the same split, or of
 * reconnecting, it fails the task, so a dead connection restarts loudly instead
 * of looking like an empty partition. An empty partition returns immediately; it
 * never reaches the timeout.
 *
 * <p>All fields accessed only from the Flink mailbox thread — no synchronization needed.
 */
public class IggySourceReader<T> implements SourceReader<T, IggySplit> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySourceReader.class);

    static final int MAX_CONSECUTIVE_FAILURES = 3;

    private final SourceReaderContext context;
    private final IggyConnectionConfig connectionConfig;
    private final Supplier<IggyPoller> connector;
    private final long pollBackoffMs;
    private final long pollTimeoutMs;
    private final int batchSize;
    private final long consumerId;
    private final IggyDeserializationSchema<T> deserializer;
    private final IggyStaleOffsetPolicy staleOffsetPolicy;

    // Accessed only from the Flink mailbox thread.
    private IggyPoller poller;
    private int connectFailures;
    private final Map<String, Integer> splitFailures = new HashMap<>();
    private Counter staleOffsetResets;
    private ExecutorService pollExecutor;
    private final List<IggySplit> assignedSplits = new ArrayList<>();
    private CompletableFuture<Void> availability = new CompletableFuture<>();
    private int nextSplitIndex;

    private Counter numRecordsIn;
    private Counter numBytesIn;

    IggySourceReader(
            SourceReaderContext context,
            IggyConnectionConfig connectionConfig,
            long pollBackoffMs,
            long pollTimeoutMs,
            int batchSize,
            long consumerId,
            IggyDeserializationSchema<T> deserializer) {
        this(context, connectionConfig, () -> IggyPoller.connect(connectionConfig),
                pollBackoffMs, pollTimeoutMs, batchSize, consumerId, deserializer,
                IggyStaleOffsetPolicy.EARLIEST);
    }

    IggySourceReader(
            SourceReaderContext context,
            IggyConnectionConfig connectionConfig,
            long pollBackoffMs,
            long pollTimeoutMs,
            int batchSize,
            long consumerId,
            IggyDeserializationSchema<T> deserializer,
            IggyStaleOffsetPolicy staleOffsetPolicy) {
        this(context, connectionConfig, () -> IggyPoller.connect(connectionConfig),
                pollBackoffMs, pollTimeoutMs, batchSize, consumerId, deserializer, staleOffsetPolicy);
    }

    IggySourceReader(
            SourceReaderContext context,
            IggyConnectionConfig connectionConfig,
            Supplier<IggyPoller> connector,
            long pollBackoffMs,
            long pollTimeoutMs,
            int batchSize,
            long consumerId,
            IggyDeserializationSchema<T> deserializer,
            IggyStaleOffsetPolicy staleOffsetPolicy) {
        this.context = context;
        this.connectionConfig = connectionConfig;
        this.connector = connector;
        this.pollBackoffMs = pollBackoffMs;
        this.pollTimeoutMs = pollTimeoutMs;
        this.batchSize = batchSize;
        this.consumerId = consumerId;
        this.deserializer = deserializer;
        this.staleOffsetPolicy = staleOffsetPolicy;
    }

    @Override
    public void start() {
        LOG.info("Iggy source for {}:{} (poll timeout {}ms)",
                connectionConfig.host(), connectionConfig.port(), pollTimeoutMs);
        numRecordsIn = context.metricGroup().counter("numRecordsIn");
        numBytesIn = context.metricGroup().counter("numBytesIn");
        staleOffsetResets = context.metricGroup().counter("staleOffsetResets");
        this.pollExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "iggy-poll");
            t.setDaemon(true);
            return t;
        });
        // The connection is made lazily by pollNext(), under the poll deadline.
        try {
            this.deserializer.open(context.metricGroup());
        } catch (Exception e) {
            throw new RuntimeException("Failed to open the deserializer", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (assignedSplits.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (poller == null) {
            // Connect under the same deadline as a poll: a login that never answers
            // must not hang the mailbox thread.
            CompletableFuture<IggyPoller> connecting = CompletableFuture.supplyAsync(connector::get, pollExecutor);
            try {
                poller = connecting.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
                connectFailures = 0;
            } catch (TimeoutException | ExecutionException e) {
                connecting.thenAccept(IggyPoller::close);  // a connection that arrives late is closed, not orphaned
                connectFailures++;
                escalate(connectFailures, "connect", e instanceof ExecutionException ? e.getCause() : e);
                return backOff();
            } catch (InterruptedException e) {
                connecting.thenAccept(IggyPoller::close);
                Thread.currentThread().interrupt();
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        for (var i = 0; i < assignedSplits.size(); i++) {
            var idx = (nextSplitIndex + i) % assignedSplits.size();
            var split = assignedSplits.get(idx);
            var current = poller;

            PolledMessages polled;
            Future<PolledMessages> future = pollExecutor.submit(
                    () -> current.poll(split, consumerId, batchSize));
            try {
                polled = future.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);  // interrupts the SDK's blocking wait for the response
                return splitFailed(idx, split, "got no response within " + pollTimeoutMs + " ms", e);
            } catch (ExecutionException e) {
                return splitFailed(idx, split, "failed", e.getCause());
            } catch (InterruptedException e) {
                // The request may still be outstanding on this connection: never reuse it
                future.cancel(true);
                discardPoller();
                Thread.currentThread().interrupt();
                return InputStatus.NOTHING_AVAILABLE;
            }
            if (polled.messages().isEmpty()) {
                Long staleEnd;
                try {
                    staleEnd = staleEnd(current, split, polled);
                } catch (InterruptedException e) {
                    discardPoller();
                    Thread.currentThread().interrupt();
                    return InputStatus.NOTHING_AVAILABLE;
                } catch (TimeoutException e) {
                    return splitFailed(idx, split, "partition size lookup got no response within "
                            + pollTimeoutMs + " ms", e);
                } catch (ExecutionException e) {
                    return splitFailed(idx, split, "partition size lookup failed", e.getCause());
                }
                splitFailures.remove(split.splitId());
                if (staleEnd != null) {
                    resetStaleOffset(split, staleEnd);
                    nextSplitIndex = idx;  // poll this split again straight away
                    return InputStatus.MORE_AVAILABLE;
                }
            } else {
                splitFailures.remove(split.splitId());
                split.setAwaitingConfiguredStart(false);
                for (Message msg : polled.messages()) {
                    output.collect(deserializer.deserialize(msg.payload()));
                    split.setCurrentOffset(msg.header().offset().longValue() + 1);
                    numRecordsIn.inc();
                    numBytesIn.inc(msg.payload().length);
                }
                nextSplitIndex = (idx + 1) % assignedSplits.size();
                return InputStatus.MORE_AVAILABLE;
            }
        }

        // All splits empty — back off before retrying.
        nextSplitIndex = 0;
        return backOff();
    }

    /**
     * Discard the connection after a failed poll and move past the split, so one bad
     * split cannot starve the others. Fails the task once the same split has failed
     * MAX_CONSECUTIVE_FAILURES times in a row.
     */
    private InputStatus splitFailed(int idx, IggySplit split, String what, Throwable cause) throws IOException {
        discardPoller();
        nextSplitIndex = (idx + 1) % assignedSplits.size();
        int failures = splitFailures.merge(split.splitId(), 1, Integer::sum);
        escalate(failures, "poll of " + split.splitId() + " " + what, cause);
        return backOff();
    }

    private void escalate(int failures, String what, Throwable cause) throws IOException {
        if (failures >= MAX_CONSECUTIVE_FAILURES) {
            throw new IOException("Iggy " + what + ": " + failures
                    + " failures in a row; failing the task so it restarts", cause);
        }
        LOG.warn("Iggy {}; discarding the connection and reconnecting ({} of {} before failing the task)",
                what, failures, MAX_CONSECUTIVE_FAILURES, cause);
    }

    private void discardPoller() {
        if (poller != null) {
            poller.close();
            poller = null;
        }
    }

    /**
     * The partition's next offset if the split's offset is beyond it, else null.
     *
     * <p>That only happens after the topic was recreated or purged: Iggy answers a poll
     * past the end with an empty batch, not an error. A reader at the tail asks for
     * {@code currentOffset + 1}; anything higher is stale. An empty partition and one
     * holding only offset 0 both report {@code currentOffset} 0, so those cases ask the
     * server for the partition's message count. A split still sitting on a configured
     * {@code specific-offset} is left alone until it has read something: starting beyond
     * the end is what the user asked for.
     */
    private Long staleEnd(IggyPoller current, IggySplit split, PolledMessages polled)
            throws TimeoutException, ExecutionException, InterruptedException {
        if (polled.currentOffset() == null || split.isAwaitingConfiguredStart()) {
            return null;
        }
        long last = polled.currentOffset().longValue();
        long offset = split.getCurrentOffset();
        if (offset <= 1 && last == 0) {
            if (offset == 0) {
                return null;
            }
            return messagesCount(current, split) == 0 ? 0L : null;  // offset 1: stale only if empty
        }
        if (offset <= last + 1) {
            return null;
        }
        if (last > 0) {
            return last + 1;
        }
        return messagesCount(current, split) == 0 ? 0L : 1L;  // currentOffset 0: empty, or just offset 0
    }

    private long messagesCount(IggyPoller current, IggySplit split)
            throws TimeoutException, ExecutionException, InterruptedException {
        Future<Long> count = pollExecutor.submit(() -> current.messagesCount(split));
        try {
            return count.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            count.cancel(true);
            throw e;
        }
    }

    private void resetStaleOffset(IggySplit split, long end) throws IOException {
        String what = String.format("split %s is at offset %d but its partition ends at offset %d: "
                + "the topic was recreated or purged after that offset was checkpointed",
                split.splitId(), split.getCurrentOffset(), end);
        switch (staleOffsetPolicy) {
            case FAIL -> throw new IOException(what + ". Resubmit the job without its checkpoint, "
                    + "or set 'stale-offset' to 'earliest' or 'latest'.");
            case EARLIEST -> {
                LOG.warn("Iggy {}; re-reading the partition from offset 0", what);
                split.setCurrentOffset(0);
            }
            case LATEST -> {
                LOG.warn("Iggy {}; skipping to offset {}", what, end);
                split.setCurrentOffset(end);
            }
        }
        staleOffsetResets.inc();
    }

    private InputStatus backOff() {
        availability = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(pollBackoffMs, TimeUnit.MILLISECONDS)
                .execute(() -> availability.complete(null));
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public void addSplits(List<IggySplit> splits) {
        assignedSplits.addAll(splits);
        availability.complete(null);
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public List<IggySplit> snapshotState(long checkpointId) {
        return assignedSplits.stream()
                .map(s -> new IggySplit(s.getStreamId(), s.getTopicId(),
                        s.getPartitionId(), s.getCurrentOffset(), s.isAwaitingConfiguredStart()))
                .toList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void close() {
        if (pollExecutor != null) {
            pollExecutor.shutdownNow();
        }
        discardPoller();
    }
}
