package org.apache.flink.connector.iggy;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for {@link IggySplit}.
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

        int size = 4 + streamBytes.length + 4 + topicBytes.length + 4 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(streamBytes.length);
        buffer.put(streamBytes);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(split.getPartitionId());
        buffer.putLong(split.getCurrentOffset());

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

        return new IggySplit(streamId, topicId, partitionId, startingOffset);
    }
}
