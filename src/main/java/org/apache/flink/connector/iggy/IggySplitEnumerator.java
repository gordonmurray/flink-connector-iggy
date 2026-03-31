package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.PollingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Discovers Iggy partitions and assigns them round-robin to readers.
 * Periodically re-scans for partitions added at runtime.
 */
public class IggySplitEnumerator implements SplitEnumerator<IggySplit, IggyEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(IggySplitEnumerator.class);

    private final SplitEnumeratorContext<IggySplit> context;
    private final IggyConnectionConfig connectionConfig;
    private final String streamId;
    private final String topicId;
    private final long discoveryIntervalMs;
    private final IggyOffsetSpec offsetSpec;

    private IggyTcpClient client;
    private final List<IggySplit> unassignedSplits = new ArrayList<>();
    private final Map<Integer, List<IggySplit>> readerAssignments = new HashMap<>();
    private final Set<Integer> knownPartitions = new HashSet<>();

    IggySplitEnumerator(
            SplitEnumeratorContext<IggySplit> context,
            IggyConnectionConfig connectionConfig,
            String streamId, String topicId,
            long discoveryIntervalMs) {
        this(context, connectionConfig, streamId, topicId, discoveryIntervalMs,
                IggyOffsetSpec.earliest());
    }

    IggySplitEnumerator(
            SplitEnumeratorContext<IggySplit> context,
            IggyConnectionConfig connectionConfig,
            String streamId, String topicId,
            long discoveryIntervalMs,
            IggyOffsetSpec offsetSpec) {
        this.context = context;
        this.connectionConfig = connectionConfig;
        this.streamId = streamId;
        this.topicId = topicId;
        this.discoveryIntervalMs = discoveryIntervalMs;
        this.offsetSpec = offsetSpec != null ? offsetSpec : IggyOffsetSpec.earliest();
    }

    IggySplitEnumerator(
            SplitEnumeratorContext<IggySplit> context,
            IggyConnectionConfig connectionConfig,
            String streamId, String topicId,
            long discoveryIntervalMs,
            IggyOffsetSpec offsetSpec,
            IggyEnumeratorState restored) {
        this(context, connectionConfig, streamId, topicId, discoveryIntervalMs, offsetSpec);
        // Restored splits carry their persisted offsets from checkpoint.
        // The offsetSpec is NOT applied here — it only affects newly discovered partitions.
        unassignedSplits.addAll(restored.assignedSplits());
        unassignedSplits.addAll(restored.unassignedSplits());
        unassignedSplits.forEach(s -> knownPartitions.add(s.getPartitionId()));
        LOG.info("Restored enumerator with {} splits", unassignedSplits.size());
    }

    // Keep backward-compatible restore constructor (used by existing callers)
    IggySplitEnumerator(
            SplitEnumeratorContext<IggySplit> context,
            IggyConnectionConfig connectionConfig,
            String streamId, String topicId,
            long discoveryIntervalMs,
            IggyEnumeratorState restored) {
        this(context, connectionConfig, streamId, topicId, discoveryIntervalMs,
                IggyOffsetSpec.earliest(), restored);
    }

    @Override
    public void start() {
        LOG.info("Starting enumerator for {}/{}", streamId, topicId);
        this.client = connectionConfig.connect();
        discoverPartitions();
        assignPendingSplits();
        context.callAsync(
                this::checkForNewPartitions,
                this::handleNewPartitions,
                discoveryIntervalMs, discoveryIntervalMs);
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {}

    @Override
    public void addSplitsBack(List<IggySplit> splits, int subtaskId) {
        LOG.info("Reader {} failed, {} splits returned", subtaskId, splits.size());
        readerAssignments.remove(subtaskId);
        unassignedSplits.addAll(splits);
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        readerAssignments.putIfAbsent(subtaskId, new ArrayList<>());
        assignPendingSplits();
    }

    @Override
    public IggyEnumeratorState snapshotState(long checkpointId) {
        var allAssigned = readerAssignments.values().stream()
                .flatMap(Collection::stream)
                .toList();
        return new IggyEnumeratorState(allAssigned, List.copyOf(unassignedSplits));
    }

    @Override
    public void close() throws IOException {
        // IggyTcpClient 0.7.0 has no close/shutdown — relies on GC.
        client = null;
    }

    private void discoverPartitions() {
        var partitionCount = client.topics().getTopic(
                        StreamId.of(streamId), TopicId.of(topicId))
                .orElseThrow(() -> new RuntimeException(
                        "Topic not found: " + streamId + "/" + topicId))
                .partitionsCount()
                .intValue();

        for (var i = 0; i < partitionCount; i++) {
            if (knownPartitions.add(i)) {
                // offsetSpec is applied only to NEW splits created during partition discovery.
                // Splits restored from checkpoint carry their persisted offsets and are not
                // re-initialised here. See the restore constructor.
                long startOffset = resolveStartingOffset(i);
                unassignedSplits.add(new IggySplit(streamId, topicId, i, startOffset));
            }
        }
    }

    private int checkForNewPartitions() {
        return client.topics().getTopic(StreamId.of(streamId), TopicId.of(topicId))
                .map(t -> t.partitionsCount().intValue())
                .orElse(0);
    }

    private void handleNewPartitions(int currentCount, Throwable error) {
        if (error != null) {
            LOG.warn("Partition discovery failed", error);
            return;
        }
        var added = 0;
        for (var i = 0; i < currentCount; i++) {
            if (knownPartitions.add(i)) {
                long startOffset = resolveStartingOffset(i);
                unassignedSplits.add(new IggySplit(streamId, topicId, i, startOffset));
                added++;
            }
        }
        if (added > 0) {
            LOG.info("Discovered {} new partition(s)", added);
            assignPendingSplits();
        }
    }

    private long resolveStartingOffset(int partitionId) {
        return switch (offsetSpec.getPolicy()) {
            case EARLIEST -> 0L;
            case LATEST -> fetchLatestOffset(partitionId);
            case SPECIFIC_OFFSET -> offsetSpec.getSpecificOffset();
        };
    }

    private long fetchLatestOffset(int partitionId) {
        try {
            var polled = client.messages().pollMessages(
                    StreamId.of(streamId), TopicId.of(topicId),
                    Optional.of((long) partitionId),
                    Consumer.of(0L),
                    PollingStrategy.last(),
                    1L,
                    false);
            if (polled.messages().isEmpty()) {
                return 0L;
            }
            // Next offset to be written = last message offset + 1
            return polled.messages().get(0).header().offset().longValue() + 1;
        } catch (Exception e) {
            LOG.warn("Failed to fetch latest offset for partition {}, defaulting to 0", partitionId, e);
            return 0L;
        }
    }

    private void assignPendingSplits() {
        if (unassignedSplits.isEmpty() || readerAssignments.isEmpty()) {
            return;
        }
        var readers = List.copyOf(readerAssignments.keySet());
        var toAssign = List.copyOf(unassignedSplits);
        unassignedSplits.clear();
        for (var i = 0; i < toAssign.size(); i++) {
            var readerIndex = readers.get(i % readers.size());
            var split = toAssign.get(i);
            context.assignSplit(split, readerIndex);
            readerAssignments.get(readerIndex).add(split);
        }
    }
}
