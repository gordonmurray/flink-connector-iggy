package org.apache.flink.connector.iggy;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link IggyEnumeratorState}.
 *
 * <p>Format: [assignedCount:int] [assigned splits...] [unassignedCount:int] [unassigned splits...]
 * Each split is serialized using {@link IggySplitSerializer}.
 */
public class IggyEnumeratorStateSerializer implements SimpleVersionedSerializer<IggyEnumeratorState> {

    private static final int VERSION = 1;
    private final IggySplitSerializer splitSerializer = new IggySplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IggyEnumeratorState state) throws IOException {
        List<byte[]> assignedBytes = new ArrayList<>();
        int totalSize = 4; // assignedCount
        for (IggySplit split : state.assignedSplits()) {
            byte[] b = splitSerializer.serialize(split);
            assignedBytes.add(b);
            totalSize += 4 + b.length; // length prefix + data
        }

        List<byte[]> unassignedBytes = new ArrayList<>();
        totalSize += 4; // unassignedCount
        for (IggySplit split : state.unassignedSplits()) {
            byte[] b = splitSerializer.serialize(split);
            unassignedBytes.add(b);
            totalSize += 4 + b.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(assignedBytes.size());
        for (byte[] b : assignedBytes) {
            buffer.putInt(b.length);
            buffer.put(b);
        }

        buffer.putInt(unassignedBytes.size());
        for (byte[] b : unassignedBytes) {
            buffer.putInt(b.length);
            buffer.put(b);
        }

        return buffer.array();
    }

    @Override
    public IggyEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        ByteBuffer buffer = ByteBuffer.wrap(serialized);

        int assignedCount = buffer.getInt();
        List<IggySplit> assigned = new ArrayList<>(assignedCount);
        for (int i = 0; i < assignedCount; i++) {
            int len = buffer.getInt();
            byte[] splitBytes = new byte[len];
            buffer.get(splitBytes);
            assigned.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
        }

        int unassignedCount = buffer.getInt();
        List<IggySplit> unassigned = new ArrayList<>(unassignedCount);
        for (int i = 0; i < unassignedCount; i++) {
            int len = buffer.getInt();
            byte[] splitBytes = new byte[len];
            buffer.get(splitBytes);
            unassigned.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
        }

        return new IggyEnumeratorState(assigned, unassigned);
    }
}
