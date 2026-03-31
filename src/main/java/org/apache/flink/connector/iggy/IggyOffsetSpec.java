package org.apache.flink.connector.iggy;

import java.io.Serializable;

/**
 * Immutable value object pairing an {@link IggyOffsetPolicy} with its optional argument.
 *
 * <p>Used to configure where new splits begin consuming. Restored splits from
 * checkpoints always use their persisted offsets and ignore this spec.
 */
public final class IggyOffsetSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final IggyOffsetPolicy policy;
    private final long specificOffset;

    private IggyOffsetSpec(IggyOffsetPolicy policy, long specificOffset) {
        this.policy = policy;
        this.specificOffset = specificOffset;
    }

    /** Start from the first available message (offset 0). */
    public static IggyOffsetSpec earliest() {
        return new IggyOffsetSpec(IggyOffsetPolicy.EARLIEST, 0L);
    }

    /** Start from the current end of the partition, skipping all existing messages. */
    public static IggyOffsetSpec latest() {
        return new IggyOffsetSpec(IggyOffsetPolicy.LATEST, -1L);
    }

    /** Start from a specific offset value. */
    public static IggyOffsetSpec specificOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be >= 0, got: " + offset);
        }
        return new IggyOffsetSpec(IggyOffsetPolicy.SPECIFIC_OFFSET, offset);
    }

    public IggyOffsetPolicy getPolicy() {
        return policy;
    }

    public long getSpecificOffset() {
        return specificOffset;
    }
}
