package org.apache.flink.connector.iggy;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for {@link IggySplit}.
 *
 * <p>Format: [streamLen:int][stream][topicLen:int][topic][partition:int][offset:long][awaiting:byte].
 * The trailing awaiting-configured-start byte was added after version 1 shipped. It is
 * optional on read, so checkpoints written without it restore with the flag false, and
 * readers that predate it ignore it. The version stays 1 for that reason.
 */
public class IggySplitSerializer implements SimpleVersionedSerializer<IggySplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IggySplit split) throws IOException {
        byte[] streamBytes = split.getStreamId().getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = split.getTopicId().getBytes(StandardCharsets.UTF_8);

        int size = 4 + streamBytes.length + 4 + topicBytes.length + 4 + 8 + 1;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(streamBytes.length);
        buffer.put(streamBytes);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(split.getPartitionId());
        buffer.putLong(split.getCurrentOffset());
        buffer.put((byte) (split.isAwaitingConfiguredStart() ? 1 : 0));

        return buffer.array();
    }

    @Override
    public IggySplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        ByteBuffer buffer = ByteBuffer.wrap(serialized);

        int streamLen = buffer.getInt();
        byte[] streamBytes = new byte[streamLen];
        buffer.get(streamBytes);
        String streamId = new String(streamBytes, StandardCharsets.UTF_8);

        int topicLen = buffer.getInt();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicId = new String(topicBytes, StandardCharsets.UTF_8);

        int partitionId = buffer.getInt();
        long startingOffset = buffer.getLong();
        boolean awaitingConfiguredStart = buffer.hasRemaining() && buffer.get() != 0;

        return new IggySplit(streamId, topicId, partitionId, startingOffset, awaitingConfiguredStart);
    }
}
