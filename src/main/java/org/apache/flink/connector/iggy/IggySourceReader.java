package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Pulls data from Iggy partitions using round-robin polling with non-blocking backoff.
 *
 * <p>All fields accessed only from the Flink mailbox thread — no synchronization needed.
 */
public class IggySourceReader<T> implements SourceReader<T, IggySplit> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySourceReader.class);

    private final SourceReaderContext context;
    private final IggyConnectionConfig connectionConfig;
    private final long pollBackoffMs;
    private final long pollTimeoutMs;
    private final int batchSize;
    private final long consumerId;
    private final IggyDeserializationSchema<T> deserializer;

    // Accessed only from the Flink mailbox thread.
    private IggyTcpClient client;
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
        this.context = context;
        this.connectionConfig = connectionConfig;
        this.pollBackoffMs = pollBackoffMs;
        this.pollTimeoutMs = pollTimeoutMs;
        this.batchSize = batchSize;
        this.consumerId = consumerId;
        this.deserializer = deserializer;
    }

    @Override
    public void start() {
        LOG.info("Connecting to Iggy at {}:{} (poll timeout {}ms)",
                connectionConfig.host(), connectionConfig.port(), pollTimeoutMs);
        numRecordsIn = context.metricGroup().counter("numRecordsIn");
        numBytesIn = context.metricGroup().counter("numBytesIn");
        this.pollExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "iggy-poll");
            t.setDaemon(true);
            return t;
        });
        try {
            this.client = connectionConfig.connect();
            this.deserializer.open(context.metricGroup());
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to Iggy", e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (assignedSplits.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        for (var i = 0; i < assignedSplits.size(); i++) {
            var idx = (nextSplitIndex + i) % assignedSplits.size();
            var split = assignedSplits.get(idx);

            PolledMessages polled;
            try {
                CompletableFuture<PolledMessages> future = CompletableFuture.supplyAsync(
                        () -> client.messages().pollMessages(
                                StreamId.of(split.getStreamId()),
                                TopicId.of(split.getTopicId()),
                                Optional.of((long) split.getPartitionId()),
                                Consumer.of(consumerId),
                                PollingStrategy.offset(BigInteger.valueOf(split.getCurrentOffset())),
                                (long) batchSize,
                                false),
                        pollExecutor);
                polled = future.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.debug("Poll timed out for {}, partition likely empty", split.splitId());
                continue;
            } catch (ExecutionException e) {
                LOG.warn("Poll failed for {}, will retry on next cycle", split.splitId(), e.getCause());
                continue;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return InputStatus.NOTHING_AVAILABLE;
            }

            if (!polled.messages().isEmpty()) {
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
                        s.getPartitionId(), s.getCurrentOffset()))
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
        // IggyTcpClient 0.7.0 has no close/shutdown — relies on GC.
        // Monitor thread counts in long-running jobs.
        client = null;
    }
}
