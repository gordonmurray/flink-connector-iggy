package org.apache.flink.connector.iggy;

import java.math.BigInteger;
import java.util.Optional;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

/** One connection's worth of polling. A seam so the reader's failure handling can be tested. */
interface IggyPoller extends AutoCloseable {

    /** Blocking poll of one split from its current offset. */
    PolledMessages poll(IggySplit split, long consumerId, int batchSize);

    /** Number of messages in the split's partition (only needed to tell an empty partition from one with offset 0). */
    default long messagesCount(IggySplit split) {
        throw new UnsupportedOperationException("messagesCount");
    }

    @Override
    void close();

    /** The real poller: one {@link IggyTcpClient}, closed via {@link IggyClients}. */
    static IggyPoller connect(IggyConnectionConfig config) {
        IggyTcpClient client = config.connect();
        return new IggyPoller() {
            @Override
            public PolledMessages poll(IggySplit split, long consumerId, int batchSize) {
                return client.messages().pollMessages(
                        StreamId.of(split.getStreamId()),
                        TopicId.of(split.getTopicId()),
                        Optional.of((long) split.getPartitionId()),
                        Consumer.of(consumerId),
                        PollingStrategy.offset(BigInteger.valueOf(split.getCurrentOffset())),
                        (long) batchSize,
                        false);
            }

            @Override
            public long messagesCount(IggySplit split) {
                var topic = client.topics().getTopic(StreamId.of(split.getStreamId()), TopicId.of(split.getTopicId()))
                        .orElseThrow(() -> new IllegalStateException("Topic not found: " + split.splitId()));
                return topic.partitions().stream()
                        .filter(p -> p.id() == (long) split.getPartitionId())  // 0-based, like split ids
                        .findFirst()
                        .map(p -> p.messagesCount().longValue())
                        .orElseThrow(() -> new IllegalStateException("Partition not found: " + split.splitId()));
            }

            @Override
            public void close() {
                IggyClients.closeQuietly(client);
            }
        };
    }
}
