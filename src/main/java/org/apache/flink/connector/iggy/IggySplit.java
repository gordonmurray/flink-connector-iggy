package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.SourceSplit;
import java.io.Serializable;
import java.util.Objects;

/**
 * A {@link SourceSplit} for an Apache Iggy partition.
 *
 * <p>An Iggy split is defined by a stream ID, topic ID, and partition ID.
 * It tracks the current offset for consumption, which advances as messages
 * are polled and is snapshotted during Flink checkpoints.
 */
public class IggySplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamId;
    private final String topicId;
    private final int partitionId;
    private long currentOffset;

    public IggySplit(String streamId, String topicId, int partitionId, long currentOffset) {
        this.streamId = Objects.requireNonNull(streamId);
        this.topicId = Objects.requireNonNull(topicId);
        this.partitionId = partitionId;
        this.currentOffset = currentOffset;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getTopicId() {
        return topicId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    @Override
    public String splitId() {
        return String.format("%s:%s:%d", streamId, topicId, partitionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IggySplit that = (IggySplit) o;
        return partitionId == that.partitionId &&
                currentOffset == that.currentOffset &&
                Objects.equals(streamId, that.streamId) &&
                Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, topicId, partitionId, currentOffset);
    }

    @Override
    public String toString() {
        return "IggySplit{" +
                "streamId='" + streamId + '\'' +
                ", topicId='" + topicId + '\'' +
                ", partitionId=" + partitionId +
                ", currentOffset=" + currentOffset +
                '}';
    }
}
